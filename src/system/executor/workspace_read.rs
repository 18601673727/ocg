//! `workspace_read_file`: reads one UTF-8 text file inside a Workspace the
//! operator authorized, whole when it is small, or a range of its lines, and
//! nothing else. The Workspace, its path rules and its checks are shared with
//! `workspace_write_file` and the discovery tools, see
//! [`crate::system::executor::workspace_write`] and
//! [`crate::system::executor::workspace_discovery`].
//!
//! The Workspace root is chosen by the operator, never by a Provider: a call
//! carries only a path relative to it. The root is resolved and checked once,
//! by [`Workspace::authorize`], before the tool can be registered, and it is
//! refused when it is not an existing directory, when it would contain the
//! home directory or the OCG data directory, or when it lies inside the data
//! directory. The home directory is that of the effective user's account, as
//! the system's account database states it, never `HOME`, which whoever
//! starts OCG can set to anything; when it cannot be found, every root is
//! refused.
//!
//! Each call opens the root, then every component of its path, relative to
//! the directory opened before it, with `O_NOFOLLOW`, so no symbolic link is
//! ever followed, not even one swapped in while the call runs: the file read
//! is reached from the root through directories the call itself held open.
//! A path is refused before anything is opened when it is not a plain
//! relative path, or names a hidden, secret-bearing or OCG-internal location.
//! A file is read only when it is a regular file with a single link, within
//! [`MAX_FILE_BYTES`], and the same size and modification time before and
//! after the read; its contents are returned whole, or not at all.
//!
//! A ranged read, a call that names `start_line` or `line_count`, reads a
//! file of up to [`MAX_RANGED_FILE_BYTES`] with the same walk and checks. It
//! streams the file once, keeping only the lines asked for, at most
//! [`MAX_RANGE_LINES`] of them and as many as fit [`MAX_FILE_BYTES`] once
//! escaped for JSON, and returns them exactly, with their line ends. Lines
//! are numbered from 1 and end at `\n`. The result says which lines it holds,
//! how many the file has, and where to continue; a single line too long for
//! the limit is returned cut, and says so. Only the lines returned must be
//! UTF-8 text.
//!
//! What this does not establish: a bind mount or a hard-linked directory
//! inside the Workspace can still lead outside it; the root itself is
//! checked against the directory authorized, but its ancestors are resolved
//! by path; a file replaced by a device between its check and its open is
//! opened, though never read unless it is a regular file; and a change that
//! keeps a file's size and modification time is not detected. The tool is
//! meant for a trusted, local Workspace of a single operator.

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

/// The name the tool is offered and recorded under.
pub const TOOL_NAME: &str = "workspace_read_file";

/// The largest file the tool reads whole. A larger one is refused, never
/// truncated; it may be read in ranges. Also the most a ranged read returns.
pub const MAX_FILE_BYTES: usize = 32 * 1024;

/// The largest file a ranged read reads from.
pub const MAX_RANGED_FILE_BYTES: usize = 16 * 1024 * 1024;

/// The most lines one ranged read returns.
pub const MAX_RANGE_LINES: u64 = 2_000;

/// Identifies this handler's behavior in every call it records.
const HANDLER_VERSION: &str = "workspace_read_file-3";

/// The longest relative path, in bytes, and the most components, a call may
/// name.
const MAX_PATH_BYTES: usize = 1024;
const MAX_PATH_COMPONENTS: usize = 64;

/// Names that are refused wherever they appear in a path, compared without
/// case: OCG's own storage, and files that usually hold secrets.
const PROTECTED_NAMES: &[&str] = &[
    "ocg_db",
    "ocg.redb",
    "ocg_logs",
    "ocg_attachments",
    "credentials",
    "credentials.json",
    "secrets.json",
    "secrets.yaml",
    "secrets.yml",
];
/// Prefixes of private key files.
const PROTECTED_PREFIXES: &[&str] = &["id_rsa", "id_dsa", "id_ecdsa", "id_ed25519"];
/// Extensions of key, certificate store and environment files.
const PROTECTED_EXTENSIONS: &[&str] = &[
    "env", "jks", "kdbx", "key", "keystore", "p12", "pem", "pfx", "ppk",
];

// Commands can execute arbitrary project code, including writes outside their
// declared root. Conservatively invalidate every local snapshot for any OCG
// workspace mutation, and keep it dirty for the entire operation's lifetime.
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

/// A directory's identity, which a later open can be compared against.
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

/// Why a Workspace root cannot be authorized.
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
    /// The effective user's home directory is unknown, so whether the root
    /// contains it cannot be checked.
    #[error(
        "the home directory of the effective user cannot be determined from the account database, so no Workspace root can be authorized: {0}"
    )]
    HomeUnknown(HomeError),
    #[error("the Workspace root overlaps the OCG data directory")]
    OverlapsDataDirectory,
}

/// A Workspace root the operator authorized for Provider access, resolved
/// and checked. Only [`Workspace::authorize`] makes one; a clone is the same
/// authorization, for the tools of one execution to share.
#[derive(Clone, Debug)]
pub struct Workspace {
    root: PathBuf,
    identity: Identity,
    data_directory: Identity,
}

impl Workspace {
    /// Resolves `root` and checks that it may be read from: it must be an
    /// absolute path to an existing directory that neither contains the
    /// effective user's home directory, see [`account_home`], nor overlaps
    /// `data_directory`.
    ///
    /// # Errors
    ///
    /// Returns why the root is refused; [`WorkspaceError::HomeUnknown`]
    /// whatever the root when the home directory cannot be determined.
    pub fn authorize(root: &Path, data_directory: &DataDirectory) -> Result<Self, WorkspaceError> {
        Self::authorize_beside(root, data_directory, account_home())
    }

    /// [`Self::authorize`], with `home` as what [`account_home`] answered.
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

    /// The resolved root.
    pub fn root(&self) -> &Path {
        &self.root
    }

    /// The whole text of the file at `path`, relative to the root.
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

    /// Up to `line_count` lines of the file at `path`, relative to the root,
    /// from line `start_line`, see [`read_lines`].
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

    /// The whole content of the file at `path`, relative to the root, as
    /// bytes, when it has at most `limit` of them: the path rules, the walk
    /// and the checks of reading text, without requiring text. For a file
    /// the operator chose, such as an image to send.
    ///
    /// # Errors
    ///
    /// Returns why nothing was read; [`Refusal::TooLarge`] for a file over
    /// `limit`.
    pub fn read_binary(&self, path: &str, limit: usize) -> Result<Vec<u8>, Refusal> {
        let (directory, name) = self.parent(path)?;
        let file = open_within(&directory, name, Step::File, limit)?;
        read_bytes(&file, limit)
    }

    /// The directory that holds the last component of `path`, opened from
    /// the root through every directory before it, and that component. The
    /// path is checked first, and the root must still be the directory that
    /// was authorized; no directory on the way may be the data directory.
    pub(super) fn parent<'a>(&self, path: &'a str) -> Result<(File, &'a str), Refusal> {
        let components = components(path)?;
        let (name, parents) = components.split_last().ok_or(Refusal::InvalidPath)?;
        let mut directory = self.open_root()?;
        for parent in parents {
            directory = self.subdirectory(&directory, parent)?;
        }
        Ok((directory, *name))
    }

    /// The directory at `path`, relative to the root, opened from the root
    /// through every directory on the way, as [`Self::parent`] opens them;
    /// `.` names the root itself.
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

    /// The directory `name` inside `directory`, opened without following a
    /// link, unless it is the data directory.
    pub(crate) fn subdirectory(&self, directory: &File, name: &str) -> Result<File, Refusal> {
        let opened = open_at(directory, name, Step::Directory)?;
        let metadata = opened.metadata().map_err(|_| Refusal::Unreadable)?;
        if Identity::of(&metadata) == self.data_directory {
            return Err(Refusal::Protected);
        }
        Ok(opened)
    }

    /// The root, opened, when it still exists and is still the directory
    /// that was authorized.
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

/// Why the effective user's home directory is unknown.
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

/// The home directory of the effective user, resolved: the one its entry in
/// the account database names (`getpwuid_r` of the effective user ID), the
/// source the system itself uses to set `HOME` at login. The environment is
/// never consulted, so an unset, missing or misleading `HOME` changes
/// nothing. A home directory that is not absolute or does not exist is not
/// one a root can be checked against, so it is an error too.
///
/// # Errors
///
/// Returns why the home directory cannot be determined.
pub fn account_home() -> Result<PathBuf, HomeError> {
    let user = nix::unistd::User::from_uid(nix::unistd::Uid::effective())
        .map_err(|errno| HomeError::Lookup(errno.into()))?
        .ok_or(HomeError::NoAccount)?;
    if !user.dir.is_absolute() {
        return Err(HomeError::NotAbsolute);
    }
    user.dir.canonicalize().map_err(HomeError::Unresolved)
}

/// Why a call reads nothing. Its message is recorded and may reach the
/// Provider, so it never repeats the path or anything read.
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

/// The components of a plain relative path: no leading, trailing or doubled
/// separator, no `.` or `..`, no backslash, colon or control character, and
/// no hidden or protected name.
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

/// Checks one component of a path as [`components`] does.
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

/// What a component of a path must be.
#[derive(Clone, Copy, PartialEq, Eq)]
pub(crate) enum Step {
    Directory,
    File,
}

/// Opens the directory at `path`, refusing a symbolic link as its last
/// component.
fn open_directory(path: &Path) -> std::io::Result<File> {
    let flags =
        OFlags::RDONLY | OFlags::DIRECTORY | OFlags::NOFOLLOW | OFlags::NONBLOCK | OFlags::CLOEXEC;
    Ok(File::from(rustix::fs::open(path, flags, Mode::empty())?))
}

/// Opens `name` inside `directory` as `step` requires, without following a
/// symbolic link. Its type is checked first, so nothing but a directory or a
/// regular file of an acceptable size is opened; the open itself refuses a
/// link, and a directory step refuses anything but a directory, whatever
/// replaced the checked entry meanwhile.
pub(super) fn open_at(directory: &File, name: &str, step: Step) -> Result<File, Refusal> {
    open_within(directory, name, step, MAX_FILE_BYTES)
}

/// [`open_at`], with a file of up to `limit` bytes.
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

/// Reads the whole of an opened file, bounded by [`MAX_FILE_BYTES`], as text.
pub(super) fn read_whole(file: &File) -> Result<String, Refusal> {
    let content = read_bytes(file, MAX_FILE_BYTES)?;
    let text = String::from_utf8(content).map_err(|_| Refusal::NotText)?;
    if text.contains('\0') {
        return Err(Refusal::NotText);
    }
    Ok(text)
}

/// Reads the whole of an opened file, bounded by `limit`: a regular file
/// with a single link, unchanged while it was read.
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

/// The metadata of an opened file that may be read: a regular file with a
/// single link, of at most `limit` bytes.
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

/// Refuses a file that `read` bytes were read from when they are not all it
/// held, or when its size or times changed since `before`.
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

/// Lines of a file, as a ranged read returns them.
#[derive(Debug, PartialEq, Eq)]
struct LineRange {
    /// The first line asked for.
    start_line: u64,
    /// The last line returned, if any is: none when the file ends before
    /// `start_line`.
    end_line: Option<u64>,
    /// The lines returned, exactly as the file holds them.
    content: String,
    /// The one line returned is only its beginning: it alone is longer
    /// than the limit.
    line_truncated: bool,
    total_lines: u64,
}

impl LineRange {
    /// The line to read next, unless the file ends with these lines.
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

/// The bytes `byte` takes in a JSON string: it is escaped when it is a quote,
/// a backslash or a control character; a byte of a multibyte character is
/// written as it is.
fn escaped_len(byte: u8) -> usize {
    match byte {
        b'"' | b'\\' | b'\n' | b'\r' | b'\t' | 0x08 | 0x0c => 2,
        0x00..=0x1f => 6,
        _ => 1,
    }
}

/// Reads an opened file once, from its start, in a bounded buffer, keeping
/// only lines `start_line` to `start_line + line_count - 1`, and of those
/// only whole lines that fit [`MAX_FILE_BYTES`] escaped for JSON, except
/// that a first line too long for it alone is kept cut at a character
/// boundary. The file must be a regular file with a single link, within
/// [`MAX_RANGED_FILE_BYTES`] and unchanged while it was read, and the lines
/// kept must be UTF-8 text without NUL.
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
    // The line the next byte belongs to, and how many lines were kept
    // whole, ending where `kept` ends.
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
    if cut {
        // The cut may fall inside a character, which is left out.
        if let Err(error) = std::str::from_utf8(&content)
            && error.error_len().is_none()
        {
            content.truncate(error.valid_up_to());
        }
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

/// The parameters a call must match exactly: one relative path, and for a
/// ranged read, where its lines start and how many it asks for.
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

/// The range a call asks for, if it names one: its first line and how many
/// lines, each defaulted when not named. The schema already refuses other
/// values; they are checked again here.
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

/// The handler of `workspace_read_file`, bound to one authorized Workspace.
struct ReadFile {
    workspace: Workspace,
}

impl ToolHandler for ReadFile {
    fn version(&self) -> &str {
        HANDLER_VERSION
    }

    /// Reading changes nothing outside OCG. That says nothing about whether
    /// what it reads may be disclosed, which the operator decided by
    /// authorizing the Workspace.
    fn effect(&self) -> ToolEffect {
        ToolEffect::ReadOnly
    }

    /// Reads on the blocking pool while holding a file permit.
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

/// Registers `workspace_read_file` for `workspace` in `registry`, without
/// authorizing it.
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
