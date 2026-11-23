use std::fs::File;
use std::io::Write as _;
use std::sync::PoisonError;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use rustix::fs::{AtFlags, FileType, Gid, Mode, OFlags, RenameFlags, Stat};
use rustix::io::Errno;
use serde_json::{Value, json};

use super::read::{self, Refusal as PathRefusal, Step, Workspace, open_within, read_bytes};
use crate::db::entities::{ContentDigest, JobId};
use crate::workspace::ExecutionFailure as WriteFailure;
use crate::workspace::blocking::Kind;
use crate::workspace::process::{StopSignal, WorkerTracker};

pub const TOOL_NAME: &str = "workspace_write_file";

const MAX_CONTENT_BYTES: usize = 48 * 1024;
const MAX_EDITS: usize = 32;

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
    #[error("multi_replace edits overlap in the original file")]
    Overlap,
    #[error("multi_replace old and new text together exceed {MAX_CONTENT_BYTES} bytes")]
    EditsTooLarge,
    #[error("the resulting file exceeds {MAX_REPLACE_FILE_BYTES} bytes")]
    ResultTooLarge,
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

impl From<Failure> for WriteFailure {
    fn from(failure: Failure) -> Self {
        match failure {
            Failure::Refused(refusal) => Self::Refused(refusal.to_string()),
            Failure::Uncertain(uncertainty) => Self::Uncertain(uncertainty.to_string()),
        }
    }
}

struct Edit<'a> {
    old: &'a str,
    new: &'a str,
}

enum Replacements<'a> {
    Single(Edit<'a>),
    Multi(Vec<Edit<'a>>),
}

enum Operation<'a> {
    Create { content: &'a str },
    Update { content: &'a str, expected: &'a str },
    Replace(Replacements<'a>),
    Delete { expected: &'a str },
}

fn parse_edit(object: &serde_json::Map<String, Value>) -> Result<Edit<'_>, Refusal> {
    let old = object
        .get("old")
        .and_then(Value::as_str)
        .filter(|old| !old.is_empty());
    let (Some(old), Some(new)) = (old, object.get("new").and_then(Value::as_str)) else {
        return Err(Refusal::InvalidArguments);
    };
    if old.len() > MAX_CONTENT_BYTES || new.len() > MAX_CONTENT_BYTES {
        return Err(Refusal::TooLarge);
    }
    if old.contains('\0') || new.contains('\0') {
        return Err(Refusal::NotText);
    }
    Ok(Edit { old, new })
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
            Operation::Replace(Replacements::Single(parse_edit(object)?))
        }
        "multi_replace" => {
            if object.len() != 3 {
                return Err(Refusal::InvalidArguments);
            }
            let edits = object
                .get("edits")
                .and_then(Value::as_array)
                .ok_or(Refusal::InvalidArguments)?;
            if !(1..=MAX_EDITS).contains(&edits.len()) {
                return Err(Refusal::InvalidArguments);
            }
            let mut parsed = Vec::with_capacity(edits.len());
            let mut bytes = 0;
            for edit in edits {
                let object = edit
                    .as_object()
                    .filter(|fields| fields.len() == 2)
                    .ok_or(Refusal::InvalidArguments)?;
                let edit = parse_edit(object)?;
                bytes += edit.old.len() + edit.new.len();
                if bytes > MAX_CONTENT_BYTES {
                    return Err(Refusal::EditsTooLarge);
                }
                parsed.push(edit);
            }
            Operation::Replace(Replacements::Multi(parsed))
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
    let file = open_within(&directory, name, Step::File, MAX_CONTENT_BYTES)?;
    let before = rustix::fs::fstat(&file).map_err(|_| PathRefusal::Unreadable)?;
    let current = read_bytes(&file, MAX_CONTENT_BYTES)?;
    let current = std::str::from_utf8(&current).map_err(|_| PathRefusal::NotText)?;
    if current.contains('\0') {
        return Err(PathRefusal::NotText.into());
    }
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

fn unique_offset(current: &[u8], old: &str) -> Result<usize, Refusal> {
    let finder = memchr::memmem::Finder::new(old);
    let mut count = 0;
    let mut offset = 0;
    let mut search_start = 0;
    while let Some(hit) = finder.find(&current[search_start..]) {
        count += 1;
        offset = search_start + hit;
        search_start = offset + 1; // Also count overlapping matches for this nonempty needle.
    }
    if count != 1 {
        return Err(Refusal::Occurrences(count));
    }
    Ok(offset)
}

fn replace(
    workspace: &Workspace,
    path: &str,
    replacements: Replacements<'_>,
    stop: &StopSignal,
) -> Result<Value, Failure> {
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
    let edits = match &replacements {
        Replacements::Single(edit) => std::slice::from_ref(edit),
        Replacements::Multi(edits) => edits.as_slice(),
    };
    let mut spans = Vec::with_capacity(edits.len());
    for edit in edits {
        let start = unique_offset(current.as_bytes(), edit.old)?;
        spans.push((start, start + edit.old.len(), edit.new));
    }
    spans.sort_unstable_by_key(|span| span.0);
    if spans.windows(2).any(|pair| pair[0].1 > pair[1].0) {
        return Err(Refusal::Overlap.into());
    }
    let len = current.len() - edits.iter().map(|edit| edit.old.len()).sum::<usize>()
        + edits.iter().map(|edit| edit.new.len()).sum::<usize>();
    if len > MAX_REPLACE_FILE_BYTES {
        return Err(Refusal::ResultTooLarge.into());
    }
    let mut new_content = String::with_capacity(len);
    let mut cursor = 0;
    for &(start, end, new) in &spans {
        new_content.push_str(&current[cursor..start]);
        new_content.push_str(new);
        cursor = end;
    }
    new_content.push_str(&current[cursor..]);

    let before_sha256 = ContentDigest::of(current.as_bytes()).to_string();
    let after_sha256 = ContentDigest::of(new_content.as_bytes()).to_string();
    let staged = Staged::new(&directory, &new_content, Some(&read))?;
    staged.publish(
        name,
        Publication::Replace {
            file: &file,
            read: &read,
        },
        stop,
    )?;
    Ok(match replacements {
        Replacements::Single(_) => json!({
            "operation": "replace", "path": path, "offset": spans[0].0,
            "before_sha256": before_sha256, "after_sha256": after_sha256,
        }),
        Replacements::Multi(_) => json!({
            "operation": "multi_replace", "path": path, "edits_applied": spans.len(),
            "before_sha256": before_sha256, "after_sha256": after_sha256,
        }),
    })
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
            .map_err(|_| Uncertainty::NotDurable.into()) // The rename may already be visible.
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

pub(crate) fn parameters() -> Value {
    json!({
        "type": "object",
        "properties": {
            "operation": {
                "type": "string",
                "enum": ["create", "update", "replace", "multi_replace", "delete"]
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
            },
            "edits": {
                "type": "array",
                "minItems": 1,
                "maxItems": MAX_EDITS,
                "items": {
                    "type": "object",
                    "properties": {"old": {"type": "string", "minLength": 1}, "new": {"type": "string"}},
                    "required": ["old", "new"],
                    "additionalProperties": false
                }
            }
        },
        "required": ["operation", "path"],
        "oneOf": [
            {
                "properties": {"operation": {"const": "create"}, "edits": false},
                "required": ["content"],
                "not": {"anyOf": [{"required": ["expected_content"]}, {"required": ["old"]}, {"required": ["new"]}]}
            },
            {
                "properties": {"operation": {"const": "update"}, "edits": false},
                "required": ["content", "expected_content"],
                "not": {"anyOf": [{"required": ["old"]}, {"required": ["new"]}]}
            },
            {
                "properties": {"operation": {"const": "replace"}, "edits": false},
                "required": ["old", "new"],
                "not": {"anyOf": [{"required": ["content"]}, {"required": ["expected_content"]}]}
            },
            {
                "properties": {"operation": {"const": "multi_replace"}, "content": false, "expected_content": false, "old": false, "new": false},
                "required": ["edits"]
            },
            {
                "properties": {"operation": {"const": "delete"}, "edits": false},
                "required": ["expected_content"],
                "not": {"anyOf": [{"required": ["content"]}, {"required": ["old"]}, {"required": ["new"]}]}
            }
        ],
        "additionalProperties": false
    })
}

pub(crate) struct WriteFile {
    workspace: Workspace,
    workers: WorkerTracker,
}

impl WriteFile {
    pub(crate) fn new(workspace: Workspace) -> Self {
        Self {
            workspace,
            workers: Default::default(),
        }
    }

    pub(crate) async fn stop(&self, job: JobId) {
        self.workers.stop_job(job).await;
    }

    pub(crate) async fn write(
        &self,
        job: Option<JobId>,
        arguments: Value,
    ) -> Result<Value, WriteFailure> {
        let workspace = self.workspace.clone();
        self.workers
            .run_tracked(job, Kind::File, StopSignal::new(), move |stop| {
                let (path, operation) = parse(&arguments)?;
                let _mutation = read::WorkspaceMutation::begin();
                match operation {
                    Operation::Create { content } => {
                        create(&workspace, path, content, stop)?;
                        Ok(json!({
                            "operation": "create", "path": path, "bytes_written": content.len(),
                            "after_sha256": ContentDigest::of(content.as_bytes()).to_string(),
                        }))
                    }
                    Operation::Update { content, expected } => {
                        update(&workspace, path, expected, content, stop)?;
                        Ok(json!({
                            "operation": "update", "path": path, "bytes_written": content.len(),
                            "after_sha256": ContentDigest::of(content.as_bytes()).to_string(),
                        }))
                    }
                    Operation::Replace(edits) => replace(&workspace, path, edits, stop),
                    Operation::Delete { expected } => {
                        delete(&workspace, path, expected, stop)?;
                        Ok(json!({"operation": "delete", "path": path}))
                    }
                }
            })
            .await
            .map_err(|error| WriteFailure::Uncertain(error.to_string()))?
            .map_err(WriteFailure::from)
    }
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::path::PathBuf;

    use super::*;

    struct Fixture {
        directory: PathBuf,
        tool: WriteFile,
    }

    impl Fixture {
        fn new() -> Self {
            let directory =
                std::env::temp_dir().join(format!("ocg-write-{}", uuid::Uuid::now_v7()));
            fs::create_dir(&directory).unwrap();
            let root = directory.join("work");
            let data = directory.join("data");
            fs::create_dir(&root).unwrap();
            fs::create_dir(&data).unwrap();
            let workspace = Workspace::authorize(&root, &data).unwrap();
            Self {
                directory,
                tool: WriteFile::new(workspace),
            }
        }

        fn file(&self) -> PathBuf {
            self.tool.workspace.root().join("file.txt")
        }

        async fn write(&self, mut args: Value) -> Result<Value, WriteFailure> {
            args["path"] = json!("file.txt");
            self.tool.write(None, args).await
        }

        async fn refused(&self, args: Value, reason: Refusal) {
            assert_eq!(
                self.write(args).await,
                Err(WriteFailure::Refused(reason.to_string()))
            );
        }
    }

    impl Drop for Fixture {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.directory);
        }
    }

    #[ntex::test]
    async fn create_and_update_enforce_byte_limits_and_expected_content() {
        let fixture = Fixture::new();
        let full = "é".repeat(24 * 1024);
        let oversized = format!("{full}x");
        fixture
            .refused(
                json!({"operation": "create", "content": oversized}),
                Refusal::TooLarge,
            )
            .await;
        fixture
            .refused(
                json!({"operation": "create", "content": "a\u{0}b"}),
                Refusal::NotText,
            )
            .await;
        assert!(!fixture.file().exists());
        let receipt = fixture
            .write(json!({"operation": "create", "content": full}))
            .await
            .unwrap();
        assert_eq!(receipt["bytes_written"], 48 * 1024);
        assert_eq!(fs::read_to_string(fixture.file()).unwrap(), full);
        for (expected, content, reason) in [
            ("missing", "new", Refusal::Mismatch),
            (&full, &oversized, Refusal::TooLarge),
            (&oversized, &full, Refusal::TooLarge),
        ] {
            fixture
                .refused(
                    json!({"operation":"update","expected_content":expected,"content":content}),
                    reason,
                )
                .await;
            assert_eq!(fs::read_to_string(fixture.file()).unwrap(), full);
        }
        let updated = "b".repeat(48 * 1024);
        fixture
            .write(json!({"operation":"update","expected_content":full,"content":updated}))
            .await
            .unwrap();
        assert_eq!(fs::read_to_string(fixture.file()).unwrap(), updated);
    }

    #[ntex::test]
    async fn replace_requires_one_original_match_including_overlapping_occurrences() {
        let fixture = Fixture::new();
        let original = "é aaa end";
        fs::write(fixture.file(), original).unwrap();
        for (old, count) in [("other", 0), ("aa", 2)] {
            fixture
                .refused(
                    json!({"operation":"replace","old":old,"new":"!"}),
                    Refusal::Occurrences(count),
                )
                .await;
            assert_eq!(fs::read_to_string(fixture.file()).unwrap(), original);
        }
        let receipt = fixture
            .write(json!({"operation":"replace","old":"aaa","new":"✓"}))
            .await
            .unwrap();
        assert_eq!(receipt["offset"], "é ".len());
        assert_eq!(
            receipt["before_sha256"],
            ContentDigest::of(original.as_bytes()).to_string()
        );
        assert_eq!(fs::read_to_string(fixture.file()).unwrap(), "é ✓ end");
    }

    #[ntex::test]
    async fn multi_replace_resolves_all_spans_against_the_original_before_publishing() {
        let fixture = Fixture::new();
        let original = "abc-aaa-end";
        fs::write(fixture.file(), original).unwrap();
        let good = json!({"old":"end","new":"abc"});
        for (other, reason) in [
            (json!({"old":"missing","new":"!"}), Refusal::Occurrences(0)),
            (json!({"old":"aa","new":"!"}), Refusal::Occurrences(2)),
            (json!({"old":"-en","new":"!"}), Refusal::Overlap),
            (json!({"old":"abc","new":"x\u{0}"}), Refusal::NotText),
        ] {
            let args = json!({"operation":"multi_replace","edits":[good, other]});
            fixture.refused(args, reason).await;
            assert_eq!(fs::read_to_string(fixture.file()).unwrap(), original);
            assert_eq!(
                fs::read_dir(fixture.tool.workspace.root()).unwrap().count(),
                1
            );
        }
        let receipt = fixture
            .write(json!({"operation":"multi_replace","edits":[good, {"old":"abc","new":"end"}]}))
            .await
            .unwrap();
        let updated = "end-aaa-abc";
        assert_eq!(receipt["edits_applied"], 2);
        assert_eq!(
            receipt["before_sha256"],
            ContentDigest::of(original.as_bytes()).to_string()
        );
        assert_eq!(
            receipt["after_sha256"],
            ContentDigest::of(updated.as_bytes()).to_string()
        );
        assert_eq!(fs::read_to_string(fixture.file()).unwrap(), updated);
    }

    #[test]
    fn staged_replace_preserves_a_concurrent_edit_and_removes_its_own_copy() {
        let fixture = Fixture::new();
        let root = fixture.tool.workspace.root();
        let directory = File::open(root).unwrap();
        fs::write(fixture.file(), "original").unwrap();
        let before = File::open(fixture.file()).unwrap();
        let version_read = rustix::fs::fstat(&before).unwrap();
        let staged = Staged::new(&directory, "edited", Some(&version_read)).unwrap();
        let staged_path = root.join(&staged.name);
        assert_eq!(fs::read_to_string(&staged_path).unwrap(), "edited");
        assert_eq!(fs::read_to_string(fixture.file()).unwrap(), "original");

        let concurrent = root.join("concurrent");
        fs::write(&concurrent, "published by another writer").unwrap();
        fs::rename(concurrent, fixture.file()).unwrap();
        let result = staged.publish(
            "file.txt",
            Publication::Replace {
                file: &before,
                read: &version_read,
            },
            &StopSignal::new(),
        );
        assert_eq!(result, Err(Failure::Refused(Refusal::Changed)));
        assert_eq!(
            fs::read_to_string(fixture.file()).unwrap(),
            "published by another writer"
        );
        assert!(!staged_path.exists());
    }

    #[ntex::test]
    async fn multi_replace_enforces_hunk_count_aggregate_bytes_and_result_size() {
        let fixture = Fixture::new();
        let words: Vec<_> = (0..32).map(|index| format!("[{index:02}]")).collect();
        let original = words.concat();
        fs::write(fixture.file(), &original).unwrap();
        let mut edits: Vec<_> = words
            .iter()
            .map(|old| json!({"old":old,"new":"x"}))
            .collect();
        edits.push(json!({"old":"missing","new":"!"}));
        for invalid in [json!(edits), json!([])] {
            fixture
                .refused(
                    json!({"operation":"multi_replace","edits":invalid}),
                    Refusal::InvalidArguments,
                )
                .await;
            assert_eq!(fs::read_to_string(fixture.file()).unwrap(), original);
        }
        edits.pop();
        let receipt = fixture
            .write(json!({"operation":"multi_replace","edits":edits}))
            .await
            .unwrap();
        assert_eq!(receipt["edits_applied"], 32);
        assert_eq!(fs::read_to_string(fixture.file()).unwrap(), "x".repeat(32));

        let limit = 48 * 1024;
        fs::write(fixture.file(), "a").unwrap();
        let one_edit =
            |new: &str| json!({"operation":"multi_replace","edits":[{"old":"a","new":new}]});
        fixture
            .refused(one_edit(&"b".repeat(limit)), Refusal::EditsTooLarge)
            .await;
        assert_eq!(fs::read_to_string(fixture.file()).unwrap(), "a");
        let replacement = "b".repeat(limit - 1);
        assert_eq!(
            fixture.write(one_edit(&replacement)).await.unwrap()["edits_applied"],
            1
        );
        assert_eq!(fs::read_to_string(fixture.file()).unwrap(), replacement);

        let full = format!("a{}", "b".repeat(1024 * 1024 - 1));
        fs::write(fixture.file(), &full).unwrap();
        fixture
            .refused(one_edit("aa"), Refusal::ResultTooLarge)
            .await;
        assert_eq!(fs::read_to_string(fixture.file()).unwrap(), full);
    }
}
