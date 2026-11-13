use std::fs::File;
use std::io::Read;
use std::os::unix::fs::MetadataExt;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use rustix::fs::{AtFlags, FileType, Mode, OFlags};
use rustix::io::Errno;
use serde_json::{Value, json};

use crate::db::entities::ToolEffect;
use crate::system::data_directory::DataDirectory;
use crate::system::executor::blocking::{self, Kind};
use crate::system::executor::tools::{
    ToolFailure, ToolFuture, ToolHandler, ToolRegistry, ToolRegistryError,
};

pub const TOOL_NAME: &str = "workspace_read_file";

pub const MAX_FILE_BYTES: usize = 32 * 1024;

pub const MAX_RANGED_FILE_BYTES: usize = 16 * 1024 * 1024;

pub const MAX_RANGE_LINES: u64 = 2_000;

const HANDLER_VERSION: &str = "workspace_read_file-3";

const MAX_PATH_BYTES: usize = 1024;
const MAX_PATH_COMPONENTS: usize = 64;

const PROTECTED_NAMES: &[&str] = &[
    "ocg_db",
    "ocg.redb",
    "ocg.key",
    "ocg_logs",
    "ocg_attachments",
    "credentials",
    "credentials.json",
    "secrets.json",
    "secrets.yaml",
    "secrets.yml",
];
const PROTECTED_PREFIXES: &[&str] = &["id_rsa", "id_dsa", "id_ecdsa", "id_ed25519"];
const PROTECTED_EXTENSIONS: &[&str] = &[
    "env", "jks", "kdbx", "key", "keystore", "p12", "pem", "pfx", "ppk",
];

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) struct MutationStamp {
    revision: u128,
    active: usize,
}

static MUTATIONS: std::sync::Mutex<MutationStamp> = std::sync::Mutex::new(MutationStamp {
    revision: 0,
    active: 0,
});

pub(crate) fn mutation_stamp() -> MutationStamp {
    *MUTATIONS
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

impl MutationStamp {
    pub(crate) fn is_dirty(self) -> bool {
        let current = mutation_stamp();
        self.active != 0 || current.active != 0 || self.revision != current.revision
    }
}

pub(crate) struct WorkspaceMutation;

impl WorkspaceMutation {
    pub(crate) fn begin() -> Self {
        let mut state = MUTATIONS
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        state.revision += 1;
        state.active += 1;
        Self
    }
}

impl Drop for WorkspaceMutation {
    fn drop(&mut self) {
        let mut state = MUTATIONS
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        state.revision += 1;
        state.active -= 1;
    }
}

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

#[derive(Debug, thiserror::Error)]
pub enum WorkspaceError {
    #[error("the Workspace root must be an absolute path")]
    NotAbsolute,
    #[error("the Workspace root cannot be resolved: {0}")]
    Unresolved(#[source] std::io::Error),
    #[error("the Workspace root is not a directory")]
    NotDirectory,
    #[error("the Workspace root may not be the filesystem root or contain the home directory")]
    TooBroad,
    #[error(
        "the home directory of the effective user cannot be determined from the account database, so no Workspace root can be authorized: {0}"
    )]
    HomeUnknown(HomeError),
    #[error("the Workspace root overlaps the OCG data directory")]
    OverlapsDataDirectory,
}

#[derive(Clone, Debug)]
pub struct Workspace {
    root: PathBuf,
    identity: Identity,
    data_directory: Identity,
}

impl Workspace {
    pub fn authorize(root: &Path, data_directory: &DataDirectory) -> Result<Self, WorkspaceError> {
        Self::authorize_beside(root, data_directory, account_home())
    }

    fn authorize_beside(
        root: &Path,
        data_directory: &DataDirectory,
        home: Result<PathBuf, HomeError>,
    ) -> Result<Self, WorkspaceError> {
        let home = home.map_err(WorkspaceError::HomeUnknown)?;
        if !root.is_absolute() {
            return Err(WorkspaceError::NotAbsolute);
        }
        let root = root.canonicalize().map_err(WorkspaceError::Unresolved)?;
        let directory = open_directory(&root).map_err(WorkspaceError::Unresolved)?;
        let metadata = directory.metadata().map_err(WorkspaceError::Unresolved)?;
        if !metadata.is_dir() {
            return Err(WorkspaceError::NotDirectory);
        }
        if root.parent().is_none() || home.starts_with(&root) {
            return Err(WorkspaceError::TooBroad);
        }
        let data = data_directory.root();
        if data.starts_with(&root) || root.starts_with(data) {
            return Err(WorkspaceError::OverlapsDataDirectory);
        }
        let data_metadata = std::fs::metadata(data).map_err(WorkspaceError::Unresolved)?;
        Ok(Self {
            root,
            identity: Identity::of(&metadata),
            data_directory: Identity::of(&data_metadata),
        })
    }

    pub fn root(&self) -> &Path {
        &self.root
    }

    fn read(&self, path: &str) -> Result<String, Refusal> {
        let (directory, name) = self.parent(path)?;
        let file = open_within(&directory, name, Step::File, MAX_RANGED_FILE_BYTES).map_err(
            |refusal| match refusal {
                Refusal::TooLarge { .. } => Refusal::TooLargeForRange,
                other => other,
            },
        )?;
        let metadata = readable(&file, MAX_RANGED_FILE_BYTES).map_err(|refusal| match refusal {
            Refusal::TooLarge { .. } => Refusal::TooLargeForRange,
            other => other,
        })?;
        if metadata.len() > MAX_FILE_BYTES as u64 {
            return Err(Refusal::TooLargeToReadWhole {
                bytes: metadata.len(),
            });
        }
        read_whole(&file)
    }

    fn read_range(
        &self,
        path: &str,
        start_line: u64,
        line_count: u64,
    ) -> Result<LineRange, Refusal> {
        if start_line == 0 || line_count == 0 || line_count > MAX_RANGE_LINES {
            return Err(Refusal::InvalidRange);
        }
        let (directory, name) = self.parent(path)?;
        let file = open_within(&directory, name, Step::File, MAX_RANGED_FILE_BYTES).map_err(
            |refusal| match refusal {
                Refusal::TooLarge { .. } => Refusal::TooLargeForRange,
                other => other,
            },
        )?;
        read_lines(&file, start_line, line_count)
    }

    pub(super) fn snapshot_range(&self, path: &str, line: u64) -> Result<Value, Refusal> {
        match self.read(path) {
            Ok(content) => {
                let count = content.matches('\n').count() as u64
                    + u64::from(!content.is_empty() && !content.ends_with('\n'));
                if count > MAX_RANGE_LINES {
                    return self
                        .read_range(path, line.saturating_sub(8).max(1), 80)
                        .map(|range| range.to_json(path));
                }
                Ok(json!({
                    "path": path, "start_line": 1,
                    "end_line": (count > 0).then_some(count), "total_lines": count,
                    "content": content, "line_truncated": false,
                }))
            }
            Err(Refusal::TooLargeToReadWhole { .. }) => {
                let range = self.read_range(path, line.saturating_sub(8).max(1), 80)?;
                Ok(range.to_json(path))
            }
            Err(error) => Err(error),
        }
    }

    pub fn read_binary(&self, path: &str, limit: usize) -> Result<Vec<u8>, Refusal> {
        let (directory, name) = self.parent(path)?;
        let file = open_within(&directory, name, Step::File, limit)?;
        read_bytes(&file, limit)
    }

    pub(super) fn parent<'a>(&self, path: &'a str) -> Result<(File, &'a str), Refusal> {
        let components = components(path)?;
        let (name, parents) = components.split_last().ok_or(Refusal::InvalidPath)?;
        let mut directory = self.open_root()?;
        for parent in parents {
            directory = self.subdirectory(&directory, parent)?;
        }
        Ok((directory, *name))
    }

    pub(super) fn directory(&self, path: &str) -> Result<File, Refusal> {
        let components = if path == "." {
            Vec::new()
        } else {
            components(path)?
        };
        let mut directory = self.open_root()?;
        for component in components {
            directory = self.subdirectory(&directory, component)?;
        }
        Ok(directory)
    }

    pub(crate) fn subdirectory(&self, directory: &File, name: &str) -> Result<File, Refusal> {
        let opened = open_at(directory, name, Step::Directory)?;
        let metadata = opened.metadata().map_err(|_| Refusal::Unreadable)?;
        if Identity::of(&metadata) == self.data_directory {
            return Err(Refusal::Protected);
        }
        Ok(opened)
    }

    pub(crate) fn open_root(&self) -> Result<File, Refusal> {
        let directory = open_directory(&self.root).map_err(|_| Refusal::WorkspaceChanged)?;
        let metadata = directory
            .metadata()
            .map_err(|_| Refusal::WorkspaceChanged)?;
        if Identity::of(&metadata) != self.identity {
            return Err(Refusal::WorkspaceChanged);
        }
        Ok(directory)
    }
}

#[derive(Debug, thiserror::Error)]
pub enum HomeError {
    #[error("the account lookup failed: {0}")]
    Lookup(#[source] std::io::Error),
    #[error("the effective user has no account entry")]
    NoAccount,
    #[error("the account's home directory is not an absolute path")]
    NotAbsolute,
    #[error("the account's home directory cannot be resolved: {0}")]
    Unresolved(#[source] std::io::Error),
}

pub fn account_home() -> Result<PathBuf, HomeError> {
    let user = nix::unistd::User::from_uid(nix::unistd::Uid::effective())
        .map_err(|errno| HomeError::Lookup(errno.into()))?
        .ok_or(HomeError::NoAccount)?;
    if !user.dir.is_absolute() {
        return Err(HomeError::NotAbsolute);
    }
    user.dir.canonicalize().map_err(HomeError::Unresolved)
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, thiserror::Error)]
pub enum Refusal {
    #[error("the path is not a plain relative path inside the Workspace")]
    InvalidPath,
    #[error("the path names a hidden, secret-bearing or OCG-internal location")]
    Protected,
    #[error("the Workspace root is no longer the directory that was authorized")]
    WorkspaceChanged,
    #[error("no such file in the Workspace")]
    NotFound,
    #[error("permission to read the file was denied")]
    PermissionDenied,
    #[error("the path traverses a symbolic link")]
    SymbolicLink,
    #[error("a component of the path is not a directory")]
    NotDirectory,
    #[error("the path does not name a regular file")]
    NotRegularFile,
    #[error("the file has more than one link")]
    HardLinked,
    #[error("the file is larger than {limit} bytes")]
    TooLarge { limit: u64 },
    #[error(
        "the file is {bytes} bytes, more than the {MAX_FILE_BYTES} a whole read returns; read it in ranges with start_line and line_count, up to {MAX_RANGE_LINES} lines and about {MAX_FILE_BYTES} bytes per call"
    )]
    TooLargeToReadWhole { bytes: u64 },
    #[error("the file is larger than {MAX_RANGED_FILE_BYTES} bytes, the most a ranged read reads")]
    TooLargeForRange,
    #[error(
        "the range must name a start_line of at least 1 and a line_count from 1 to {MAX_RANGE_LINES}"
    )]
    InvalidRange,
    #[error("the file is not UTF-8 text")]
    NotText,
    #[error("the file changed while it was read")]
    Changed,
    #[error("the file could not be read")]
    Unreadable,
}

impl Refusal {
    pub(crate) fn from_errno(errno: Errno) -> Self {
        match errno {
            Errno::NOENT => Self::NotFound,
            Errno::ACCESS | Errno::PERM => Self::PermissionDenied,
            Errno::LOOP | Errno::MLINK => Self::SymbolicLink,
            Errno::NOTDIR => Self::NotDirectory,
            _ => Self::Unreadable,
        }
    }
}

pub(crate) fn components(path: &str) -> Result<Vec<&str>, Refusal> {
    if path.is_empty() || path.len() > MAX_PATH_BYTES {
        return Err(Refusal::InvalidPath);
    }
    let components = path.split('/').collect::<Vec<_>>();
    if components.len() > MAX_PATH_COMPONENTS {
        return Err(Refusal::InvalidPath);
    }
    for component in &components {
        check_component(component)?;
    }
    Ok(components)
}

pub(crate) fn check_component(component: &str) -> Result<(), Refusal> {
    if component.is_empty()
        || component == "."
        || component == ".."
        || component
            .chars()
            .any(|character| matches!(character, '\\' | ':') || character.is_control())
    {
        return Err(Refusal::InvalidPath);
    }
    if is_protected(component) {
        return Err(Refusal::Protected);
    }
    Ok(())
}

fn is_protected(component: &str) -> bool {
    let name = component.to_ascii_lowercase();
    name.starts_with('.')
        || PROTECTED_NAMES.contains(&name.as_str())
        || PROTECTED_PREFIXES
            .iter()
            .any(|prefix| name.starts_with(prefix))
        || name
            .rsplit_once('.')
            .is_some_and(|(_, extension)| PROTECTED_EXTENSIONS.contains(&extension))
}

#[derive(Clone, Copy, PartialEq, Eq)]
pub(crate) enum Step {
    Directory,
    File,
}

fn open_directory(path: &Path) -> std::io::Result<File> {
    let flags =
        OFlags::RDONLY | OFlags::DIRECTORY | OFlags::NOFOLLOW | OFlags::NONBLOCK | OFlags::CLOEXEC;
    Ok(File::from(rustix::fs::open(path, flags, Mode::empty())?))
}

pub(super) fn open_at(directory: &File, name: &str, step: Step) -> Result<File, Refusal> {
    open_within(directory, name, step, MAX_FILE_BYTES)
}

pub(crate) fn open_within(
    directory: &File,
    name: &str,
    step: Step,
    limit: usize,
) -> Result<File, Refusal> {
    let stat = rustix::fs::statat(directory, name, AtFlags::SYMLINK_NOFOLLOW)
        .map_err(Refusal::from_errno)?;
    match (FileType::from_raw_mode(stat.st_mode), step) {
        (FileType::Symlink, _) => return Err(Refusal::SymbolicLink),
        (FileType::Directory, Step::Directory) => {}
        (FileType::RegularFile, Step::File) => {
            if !u64::try_from(stat.st_size).is_ok_and(|size| size <= limit as u64) {
                return Err(Refusal::TooLarge {
                    limit: limit as u64,
                });
            }
        }
        (_, Step::Directory) => return Err(Refusal::NotDirectory),
        (_, Step::File) => return Err(Refusal::NotRegularFile),
    }
    let flags = OFlags::RDONLY
        | OFlags::NOFOLLOW
        | OFlags::NONBLOCK
        | OFlags::CLOEXEC
        | match step {
            Step::Directory => OFlags::DIRECTORY,
            Step::File => OFlags::NOCTTY,
        };
    rustix::fs::openat(directory, name, flags, Mode::empty())
        .map(File::from)
        .map_err(Refusal::from_errno)
}

pub(super) fn read_whole(file: &File) -> Result<String, Refusal> {
    let content = read_bytes(file, MAX_FILE_BYTES)?;
    let text = String::from_utf8(content).map_err(|_| Refusal::NotText)?;
    if text.contains('\0') {
        return Err(Refusal::NotText);
    }
    Ok(text)
}

pub(crate) fn read_bytes(file: &File, limit: usize) -> Result<Vec<u8>, Refusal> {
    let before = readable(file, limit)?;
    let size = usize::try_from(before.len()).unwrap_or(limit);
    let mut content = Vec::with_capacity(size.min(limit) + 1);
    file.take(limit as u64 + 1)
        .read_to_end(&mut content)
        .map_err(|_| Refusal::Unreadable)?;
    if content.len() > limit {
        return Err(Refusal::TooLarge {
            limit: limit as u64,
        });
    }
    unchanged(file, &before, content.len() as u64)?;
    Ok(content)
}

fn readable(file: &File, limit: usize) -> Result<std::fs::Metadata, Refusal> {
    let metadata = file.metadata().map_err(|_| Refusal::Unreadable)?;
    if !metadata.is_file() {
        return Err(Refusal::NotRegularFile);
    }
    if metadata.nlink() != 1 {
        return Err(Refusal::HardLinked);
    }
    if metadata.len() > limit as u64 {
        return Err(Refusal::TooLarge {
            limit: limit as u64,
        });
    }
    Ok(metadata)
}

fn unchanged(file: &File, before: &std::fs::Metadata, read: u64) -> Result<(), Refusal> {
    let after = file.metadata().map_err(|_| Refusal::Unreadable)?;
    let state = |metadata: &std::fs::Metadata| {
        (
            metadata.len(),
            metadata.mtime(),
            metadata.mtime_nsec(),
            metadata.ctime(),
            metadata.ctime_nsec(),
        )
    };
    if read != before.len() || state(before) != state(&after) {
        return Err(Refusal::Changed);
    }
    Ok(())
}

#[derive(Debug, PartialEq, Eq)]
struct LineRange {
    start_line: u64,
    end_line: Option<u64>,
    content: String,
    line_truncated: bool,
    total_lines: u64,
}

impl LineRange {
    fn next_start_line(&self) -> Option<u64> {
        let last = self.end_line.unwrap_or(self.start_line - 1);
        (last < self.total_lines).then_some(last + 1)
    }

    fn to_json(&self, path: &str) -> Value {
        json!({
            "path": path,
            "start_line": self.start_line,
            "end_line": self.end_line,
            "total_lines": self.total_lines,
            "next_start_line": self.next_start_line(),
            "line_truncated": self.line_truncated,
            "content": self.content,
        })
    }
}

fn escaped_len(byte: u8) -> usize {
    match byte {
        b'"' | b'\\' | b'\n' | b'\r' | b'\t' | 0x08 | 0x0c => 2,
        0x00..=0x1f => 6,
        _ => 1,
    }
}

fn read_lines(file: &File, start_line: u64, line_count: u64) -> Result<LineRange, Refusal> {
    let before = readable(file, MAX_RANGED_FILE_BYTES).map_err(|refusal| match refusal {
        Refusal::TooLarge { .. } => Refusal::TooLargeForRange,
        other => other,
    })?;
    let end = start_line.saturating_add(line_count);
    let mut reader = file.take(MAX_RANGED_FILE_BYTES as u64 + 1);
    let mut buffer = [0; 8 * 1024];
    let mut content = Vec::new();
    let mut escaped = 0;
    let mut line = 1;
    let mut whole = 0;
    let mut kept = 0;
    let mut full = false;
    let mut cut = false;
    let mut read = 0_u64;
    let mut last = b'\n';
    loop {
        let count = match reader.read(&mut buffer) {
            Ok(0) => break,
            Ok(count) => count,
            Err(error) if error.kind() == std::io::ErrorKind::Interrupted => continue,
            Err(_) => return Err(Refusal::Unreadable),
        };
        read += count as u64;
        if read > MAX_RANGED_FILE_BYTES as u64 {
            return Err(Refusal::TooLargeForRange);
        }
        for &byte in &buffer[..count] {
            let wanted = !full && line >= start_line && line < end;
            if wanted {
                let cost = escaped_len(byte);
                if escaped + cost > MAX_FILE_BYTES {
                    full = true;
                    if whole == 0 {
                        cut = true;
                    } else {
                        content.truncate(kept);
                    }
                } else {
                    content.push(byte);
                    escaped += cost;
                }
            }
            if byte == b'\n' {
                if wanted && !full {
                    whole += 1;
                    kept = content.len();
                }
                line += 1;
            }
        }
        last = buffer[count - 1];
    }
    unchanged(file, &before, read)?;
    if cut
        && let Err(error) = std::str::from_utf8(&content)
        && error.error_len().is_none()
    {
        content.truncate(error.valid_up_to());
    }
    let content = String::from_utf8(content).map_err(|_| Refusal::NotText)?;
    if content.contains('\0') {
        return Err(Refusal::NotText);
    }
    let total_lines = (line - 1) + u64::from(last != b'\n');
    let returned = if cut {
        1
    } else {
        content.matches('\n').count() as u64
            + u64::from(!content.is_empty() && !content.ends_with('\n'))
    };
    Ok(LineRange {
        start_line,
        end_line: (returned > 0).then(|| start_line + returned - 1),
        content,
        line_truncated: cut,
        total_lines,
    })
}

fn parameters() -> Value {
    json!({
        "type": "object",
        "properties": {
            "path": {
                "type": "string",
                "description": "A path relative to the authorized Workspace root"
            },
            "start_line": {
                "type": "integer",
                "minimum": 1,
                "description": "Ranged read: the first line to return, counting from 1; defaults to 1"
            },
            "line_count": {
                "type": "integer",
                "minimum": 1,
                "maximum": MAX_RANGE_LINES,
                "description": "Ranged read: the most lines to return; defaults to the maximum, 2000"
            }
        },
        "required": ["path"],
        "additionalProperties": false
    })
}

fn requested_range(arguments: &Value) -> Result<Option<(u64, u64)>, Refusal> {
    let field = |name: &str| match arguments.get(name) {
        None => Ok(None),
        Some(value) => value.as_u64().map(Some).ok_or(Refusal::InvalidRange),
    };
    match (field("start_line")?, field("line_count")?) {
        (None, None) => Ok(None),
        (start_line, line_count) => Ok(Some((
            start_line.unwrap_or(1),
            line_count.unwrap_or(MAX_RANGE_LINES),
        ))),
    }
}

struct ReadFile {
    workspace: Workspace,
}

impl ToolHandler for ReadFile {
    fn version(&self) -> &str {
        HANDLER_VERSION
    }

    fn effect(&self) -> ToolEffect {
        ToolEffect::ReadOnly
    }

    fn call(&self, arguments: Value) -> ToolFuture<'_> {
        Box::pin(async move {
            let refused = |refusal: Refusal| ToolFailure::not_applied(refusal.to_string());
            let Some(path) = arguments.get("path").and_then(Value::as_str) else {
                return Err(refused(Refusal::InvalidPath));
            };
            match requested_range(&arguments).map_err(refused)? {
                None => {
                    let owned_path = path.to_owned();
                    let workspace = self.workspace.clone();
                    let content = blocking::run(Kind::File, move || workspace.read(&owned_path))
                        .await
                        .map_err(ToolFailure::uncertain)?
                        .map_err(refused)?;
                    Ok(json!({"path": path, "content": content}))
                }
                Some((start_line, line_count)) => {
                    let owned_path = path.to_owned();
                    let workspace = self.workspace.clone();
                    let range = blocking::run(Kind::File, move || {
                        workspace.read_range(&owned_path, start_line, line_count)
                    })
                    .await
                    .map_err(ToolFailure::uncertain)?
                    .map_err(refused)?;
                    Ok(range.to_json(path))
                }
            }
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
            "Read one UTF-8 text file from the authorized Workspace. With only `path`, return its complete contents; a file over 32 KiB fails the call, so read larger files in ranges. With `start_line` or `line_count`, return whole lines from `start_line` (from 1) of a file of up to 16 MiB, at most `line_count` (up to 2000) and about 32 KiB of them, exactly as the file holds them, with `end_line` (null when the file ends before `start_line`), `total_lines`, and `next_start_line` to continue from (null at the end of the file); `line_truncated` says the one line returned is cut because it alone is longer than the limit.",
        ),
        parameters(),
        Arc::new(ReadFile { workspace }),
    )
}
