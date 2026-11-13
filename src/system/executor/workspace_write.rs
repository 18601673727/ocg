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

pub const TOOL_NAME: &str = "workspace_write_file";

const MAX_CONTENT_BYTES: usize = 8 * 1024;

const HANDLER_VERSION: &str = "workspace_write_file-3";

const MAX_DELETE_BYTES: usize = 64 * 1024;

const MAX_REPLACE_FILE_BYTES: usize = 1024 * 1024;

const STAGING_PREFIX: &str = ".ocg-write-";

const STAGING_ATTEMPTS: usize = 8;

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

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Operation<'a> {
    Create { content: &'a str },
    Update { content: &'a str, expected: &'a str },
    Replace { old: &'a str, new: &'a str },
    Delete { expected: &'a str },
}

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

fn parent<'a>(workspace: &Workspace, path: &'a str) -> Result<(File, &'a str), Refusal> {
    workspace.parent(path).map_err(|refusal| match refusal {
        PathRefusal::NotFound => Refusal::MissingDirectory,
        other => Refusal::Path(other),
    })
}

static TARGETS: std::sync::Mutex<std::collections::BTreeSet<(u64, u64, String)>> =
    std::sync::Mutex::new(std::collections::BTreeSet::new());
static TARGETS_FREED: std::sync::Condvar = std::sync::Condvar::new();

struct TargetLock(Option<(u64, u64, String)>);

impl TargetLock {
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

fn create(
    workspace: &Workspace,
    path: &str,
    content: &str,
    stop: &StopSignal,
) -> Result<(), Failure> {
    let (directory, name) = parent(workspace, path)?;
    let _target = TargetLock::acquire(&directory, name, stop)?;
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

enum Publication<'a> {
    Exclusive,
    Replace { file: &'a File, read: &'a Stat },
}

struct Staged<'a> {
    directory: &'a File,
    name: String,
    file: File,
    settled: bool,
}

impl<'a> Staged<'a> {
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

    fn abandon(mut self, refusal: Refusal) -> Failure {
        self.settled = true;
        if self.remove() {
            Failure::Refused(refusal)
        } else {
            Failure::Uncertain(Uncertainty::StagingLeft)
        }
    }

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

struct WriteFile {
    workspace: Workspace,
    workers: blocking::WorkerTracker,
}

impl ToolHandler for WriteFile {
    fn version(&self) -> &str {
        HANDLER_VERSION
    }

    fn effect(&self) -> ToolEffect {
        ToolEffect::NonIdempotent
    }

    fn spawns(&self) -> bool {
        true
    }

    fn call(&self, arguments: Value) -> ToolFuture<'_> {
        self.write(None, arguments)
    }

    fn call_for(&self, call: ToolCallRef, arguments: Value) -> ToolFuture<'_> {
        self.write(Some(call.job), arguments)
    }

    fn stop(&self, job: JobId) -> Pin<Box<dyn Future<Output = Result<(), ToolFailure>> + '_>> {
        Box::pin(async move {
            self.workers.stop_job(job).await;
            Ok(())
        })
    }
}

impl WriteFile {
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
