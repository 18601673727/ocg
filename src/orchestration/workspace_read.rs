//! `workspace_read_file`: reads one UTF-8 text file inside a Workspace the
//! operator authorized, whole when it is small, or a range of its lines, and
//! nothing else. The Workspace, its path rules and its checks are shared with
//! `workspace_write_file` and the discovery tools, see
//! [`crate::orchestration::workspace_write`] and
//! [`crate::orchestration::workspace_discovery`].
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

use crate::data_directory::DataDirectory;
use crate::orchestration::blocking::{self, Kind};
use crate::orchestration::tools::{
    ToolFailure, ToolFuture, ToolHandler, ToolRegistry, ToolRegistryError,
};
use crate::persistence::ToolEffect;

/// The name the tool is offered and recorded under.
pub const TOOL_NAME: &str = "workspace_read_file";

pub const WORKSPACE_ROOT_VARIABLE: &str = "OCG_WORKSPACE_ROOT";

/// The largest file the tool reads whole. A larger one is refused, never
/// truncated; it may be read in ranges. Also the most a ranged read returns.
pub const MAX_FILE_BYTES: usize = 8 * 1024;

/// The largest file a ranged read reads from.
pub const MAX_RANGED_FILE_BYTES: usize = 16 * 1024 * 1024;

/// The most lines one ranged read returns.
pub const MAX_RANGE_LINES: u64 = 2_000;

/// Identifies this handler's behavior in every call it records.
const HANDLER_VERSION: &str = "workspace_read_file-2";

/// The longest relative path, in bytes, and the most components, a call may
/// name.
const MAX_PATH_BYTES: usize = 1024;
const MAX_PATH_COMPONENTS: usize = 64;

/// Names that are refused wherever they appear in a path, compared without
/// case: OCG's own storage, and files that usually hold secrets.
const PROTECTED_NAMES: &[&str] = &[
    "ocg_db",
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
pub(super) struct MutationStamp {
    revision: u128,
    active: usize,
}

static MUTATIONS: std::sync::Mutex<MutationStamp> = std::sync::Mutex::new(MutationStamp {
    revision: 0,
    active: 0,
});

pub(super) fn mutation_stamp() -> MutationStamp {
    *MUTATIONS
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

impl MutationStamp {
    pub(super) fn is_dirty(self) -> bool {
        let current = mutation_stamp();
        self.active != 0 || current.active != 0 || self.revision != current.revision
    }
}

pub(super) struct WorkspaceMutation;

impl WorkspaceMutation {
    pub(super) fn begin() -> Self {
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
        let file = open_at(&directory, name, Step::File)?;
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
                Refusal::TooLarge => Refusal::TooLargeForRange,
                other => other,
            },
        )?;
        read_lines(&file, start_line, line_count)
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
    pub(super) fn subdirectory(&self, directory: &File, name: &str) -> Result<File, Refusal> {
        let opened = open_at(directory, name, Step::Directory)?;
        let metadata = opened.metadata().map_err(|_| Refusal::Unreadable)?;
        if Identity::of(&metadata) == self.data_directory {
            return Err(Refusal::Protected);
        }
        Ok(opened)
    }

    /// The root, opened, when it still exists and is still the directory
    /// that was authorized.
    pub(super) fn open_root(&self) -> Result<File, Refusal> {
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
    #[error("the file is larger than {MAX_FILE_BYTES} bytes")]
    TooLarge,
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
    pub(super) fn from_errno(errno: Errno) -> Self {
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
pub(super) fn components(path: &str) -> Result<Vec<&str>, Refusal> {
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
pub(super) fn check_component(component: &str) -> Result<(), Refusal> {
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
pub(super) enum Step {
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
pub(super) fn open_within(
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
                return Err(Refusal::TooLarge);
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
pub(super) fn read_bytes(file: &File, limit: usize) -> Result<Vec<u8>, Refusal> {
    let before = readable(file, limit)?;
    let size = usize::try_from(before.len()).unwrap_or(limit);
    let mut content = Vec::with_capacity(size.min(limit) + 1);
    file.take(limit as u64 + 1)
        .read_to_end(&mut content)
        .map_err(|_| Refusal::Unreadable)?;
    if content.len() > limit {
        return Err(Refusal::TooLarge);
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
        return Err(Refusal::TooLarge);
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
        Refusal::TooLarge => Refusal::TooLargeForRange,
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

    /// Reads on a dedicated blocking worker while holding a file permit.
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
            "Read one UTF-8 text file from the authorized Workspace. With only `path`, return its complete contents; a file over 8 KiB fails the call, so read larger files in ranges. With `start_line` or `line_count`, return whole lines from `start_line` (from 1) of a file of up to 16 MiB, at most `line_count` (up to 2000) and about 8 KiB of them, exactly as the file holds them, with `end_line` (null when the file ends before `start_line`), `total_lines`, and `next_start_line` to continue from (null at the end of the file); `line_truncated` says the one line returned is cut because it alone is longer than the limit.",
        ),
        parameters(),
        Arc::new(ReadFile { workspace }),
    )
}

#[cfg(test)]
mod tests {
    use std::os::unix::fs::symlink;

    use super::*;
    use crate::test_support::{TestRoot, data_directory, temporary_directory};

    const SUITE: &str = "ocg-workspace-read";

    /// An empty Workspace under the system temp dir, removed when dropped,
    /// beside a data directory of its own.
    fn workspace(name: &str) -> (TestRoot, PathBuf, TestRoot, DataDirectory) {
        let (guard, root) = temporary_directory(SUITE, &format!("{name}-workspace"));
        let (data_root, directory) = data_directory(SUITE, name);
        (guard, root, data_root, directory)
    }

    fn write(root: &Path, path: &str, content: &[u8]) {
        let path = root.join(path);
        std::fs::create_dir_all(path.parent().expect("a parent")).expect("create parents");
        std::fs::write(path, content).expect("write");
    }

    fn read(workspace: &Workspace, path: &str) -> Result<String, Refusal> {
        workspace.read(path)
    }

    /// Runs the handler as the tool loop does, with `arguments`.
    fn call(workspace: Workspace, arguments: Value) -> Result<Value, ToolFailure> {
        let handler = ReadFile { workspace };
        crate::test_support::wait(handler.call(arguments))
    }

    #[test]
    fn relative_utf8_files_are_read_whole() {
        let (_guard, root, _data, directory) = workspace("valid");
        let text = "# Title\n\nUnicode: ünïcødé ✓, \"quotes\" and \\backslashes\\.\n";
        write(&root, "README.md", text.as_bytes());
        write(&root, "docs/guide/intro.txt", b"nested\n");
        write(&root, "empty.txt", b"");
        let at_limit = "a".repeat(MAX_FILE_BYTES);
        write(&root, "limit.txt", at_limit.as_bytes());
        let workspace = Workspace::authorize(&root, &directory).expect("authorize");

        assert_eq!(read(&workspace, "README.md").as_deref(), Ok(text));
        assert_eq!(
            read(&workspace, "docs/guide/intro.txt").as_deref(),
            Ok("nested\n")
        );
        assert_eq!(read(&workspace, "empty.txt").as_deref(), Ok(""));
        assert_eq!(
            read(&workspace, "limit.txt").as_deref(),
            Ok(at_limit.as_str())
        );
        assert_eq!(
            call(workspace, json!({"path": "README.md"})),
            Ok(json!({"path": "README.md", "content": text})),
            "the result is the relative path and the complete text"
        );
    }

    #[test]
    fn paths_that_are_not_plain_relative_paths_are_refused_before_opening() {
        let (_guard, root, _data, directory) = workspace("lexical");
        write(&root, "a/b.txt", b"b");
        let workspace = Workspace::authorize(&root, &directory).expect("authorize");
        let absolute = root.join("a/b.txt");
        for path in [
            "",
            "/etc/passwd",
            absolute.to_str().expect("UTF-8"),
            "../outside.txt",
            "a/../a/b.txt",
            "a/../../outside.txt",
            "./a/b.txt",
            "a//b.txt",
            "a/b.txt/",
            "a\\b.txt",
            "C:\\Windows\\win.ini",
            "C:/Windows/win.ini",
            "file:///etc/passwd",
            "\\\\server\\share",
            "a/b\0.txt",
            "a/b\n.txt",
        ] {
            assert_eq!(
                read(&workspace, path),
                Err(Refusal::InvalidPath),
                "{path:?}"
            );
        }
        let long = "a/".repeat(MAX_PATH_COMPONENTS) + "b.txt";
        assert_eq!(read(&workspace, &long), Err(Refusal::InvalidPath));
        assert_eq!(
            read(&workspace, &"x".repeat(MAX_PATH_BYTES + 1)),
            Err(Refusal::InvalidPath)
        );
    }

    #[test]
    fn hidden_secret_bearing_and_internal_names_are_refused() {
        let (_guard, root, _data, directory) = workspace("protected");
        for path in [
            ".env",
            ".git/config",
            ".ssh/id_rsa",
            "config/.npmrc",
            "deploy/prod.env",
            "keys/server.PEM",
            "keys/server.key",
            "keys/store.p12",
            "home/id_ed25519",
            "home/id_rsa.pub",
            "aws/credentials",
            "config/secrets.yaml",
            "backup/ocg_db/ocg.redb",
            "ocg_logs/ocg.log",
            "ocg_attachments/a.txt",
            "ocg.key",
        ] {
            write(&root, path, b"secret");
        }
        let workspace = Workspace::authorize(&root, &directory).expect("authorize");
        for path in [
            ".env",
            ".git/config",
            ".ssh/id_rsa",
            "config/.npmrc",
            "deploy/prod.env",
            "keys/server.PEM",
            "keys/server.key",
            "keys/store.p12",
            "home/id_ed25519",
            "home/id_rsa.pub",
            "aws/credentials",
            "config/secrets.yaml",
            "backup/ocg_db/ocg.redb",
            "ocg_logs/ocg.log",
            "ocg_attachments/a.txt",
            "ocg.key",
        ] {
            assert_eq!(read(&workspace, path), Err(Refusal::Protected), "{path}");
        }
    }

    #[test]
    fn missing_files_directories_and_other_types_are_refused() {
        let (_guard, root, _data, directory) = workspace("types");
        write(&root, "dir/file.txt", b"x");
        write(&root, "file.txt", b"x");
        let made = std::process::Command::new("mkfifo")
            .arg(root.join("pipe"))
            .status()
            .expect("run mkfifo");
        assert!(made.success(), "make a FIFO");
        let workspace = Workspace::authorize(&root, &directory).expect("authorize");
        assert_eq!(read(&workspace, "missing.txt"), Err(Refusal::NotFound));
        assert_eq!(read(&workspace, "nowhere/file.txt"), Err(Refusal::NotFound));
        assert_eq!(read(&workspace, "dir"), Err(Refusal::NotRegularFile));
        assert_eq!(
            read(&workspace, "pipe"),
            Err(Refusal::NotRegularFile),
            "a FIFO is never opened, so the call cannot block on it"
        );
        assert_eq!(
            read(&workspace, "file.txt/inner"),
            Err(Refusal::NotDirectory)
        );
    }

    #[test]
    fn symbolic_links_are_never_followed() {
        let (_guard, root, _data, directory) = workspace("links");
        let (_outside_guard, outside, _outside_data, _) = workspace("links-outside");
        write(&outside, "secret.txt", b"outside");
        write(&root, "inside.txt", b"inside");
        write(&root, "real/file.txt", b"real");
        symlink(outside.join("secret.txt"), root.join("escape.txt")).expect("link");
        symlink(root.join("inside.txt"), root.join("alias.txt")).expect("link");
        symlink(&outside, root.join("outside")).expect("link");
        symlink(root.join("real"), root.join("linked")).expect("link");
        symlink(directory.root(), root.join("data")).expect("link");
        let workspace = Workspace::authorize(&root, &directory).expect("authorize");

        for path in [
            "escape.txt",
            "alias.txt",
            "outside/secret.txt",
            "linked/file.txt",
            "linked",
            "data/notes.txt",
        ] {
            assert_eq!(read(&workspace, path), Err(Refusal::SymbolicLink), "{path}");
        }
        assert_eq!(read(&workspace, "real/file.txt").as_deref(), Ok("real"));
    }

    #[test]
    fn a_hard_linked_file_is_refused() {
        let (_guard, root, _data, directory) = workspace("hard-link");
        // A key planted by hard link has the same inode as the original.
        std::fs::write(directory.key_path(), b"key material").expect("write a key");
        std::fs::hard_link(directory.key_path(), root.join("notes.txt")).expect("hard link");
        let workspace = Workspace::authorize(&root, &directory).expect("authorize");
        assert_eq!(read(&workspace, "notes.txt"), Err(Refusal::HardLinked));
    }

    #[test]
    fn oversized_binary_and_invalid_utf8_files_return_no_content() {
        let (_guard, root, _data, directory) = workspace("contents");
        write(&root, "big.txt", "a".repeat(MAX_FILE_BYTES + 1).as_bytes());
        write(&root, "latin1.txt", b"caf\xe9\n");
        write(&root, "binary.bin", b"\x00\x01\x02");
        let workspace = Workspace::authorize(&root, &directory).expect("authorize");
        assert_eq!(read(&workspace, "big.txt"), Err(Refusal::TooLarge));
        assert_eq!(read(&workspace, "latin1.txt"), Err(Refusal::NotText));
        assert_eq!(read(&workspace, "binary.bin"), Err(Refusal::NotText));

        let failure = call(workspace, json!({"path": "latin1.txt"}))
            .expect_err("a failed read returns no partial result");
        assert_eq!(
            failure,
            ToolFailure::not_applied("the file is not UTF-8 text")
        );
        assert!(!failure.message.contains("latin1") && !failure.message.contains("caf"));
    }

    #[test]
    fn the_workspace_root_must_be_an_existing_directory_apart_from_ocg_data() {
        let (_guard, root, _data, directory) = workspace("roots");
        write(&root, "file.txt", b"x");
        let refused = |path: &Path| Workspace::authorize(path, &directory).err();
        assert!(matches!(
            refused(Path::new("relative/workspace")),
            Some(WorkspaceError::NotAbsolute)
        ));
        assert!(matches!(
            refused(&root.join("missing")),
            Some(WorkspaceError::Unresolved(_))
        ));
        assert!(matches!(
            refused(&root.join("file.txt")),
            Some(WorkspaceError::Unresolved(_) | WorkspaceError::NotDirectory)
        ));
        assert!(matches!(
            refused(Path::new("/")),
            Some(WorkspaceError::TooBroad)
        ));
        for overlapping in [
            directory.root().to_path_buf(),
            directory.logs_path().to_path_buf(),
            directory.root().parent().expect("a parent").to_path_buf(),
        ] {
            assert!(
                matches!(
                    refused(&overlapping),
                    Some(WorkspaceError::OverlapsDataDirectory | WorkspaceError::TooBroad)
                ),
                "{}",
                overlapping.display()
            );
        }
        assert!(matches!(
            refused(directory.logs_path()),
            Some(WorkspaceError::OverlapsDataDirectory)
        ));
        if let Some(home) = std::env::var_os("HOME") {
            assert!(matches!(
                refused(Path::new(&home)),
                Some(WorkspaceError::TooBroad | WorkspaceError::Unresolved(_))
            ));
        }
        // A symbolic link to an acceptable directory is resolved first.
        let (_link_guard, links, _link_data, _) = workspace("roots-link");
        symlink(&root, links.join("to-root")).expect("link");
        let workspace =
            Workspace::authorize(&links.join("to-root"), &directory).expect("authorize");
        assert_eq!(workspace.root(), root);
    }

    #[test]
    fn a_root_is_checked_against_the_home_directory_it_is_given() {
        let (_guard, base, _data, directory) = workspace("home-boundary");
        let home = base.join("home");
        let project = home.join("Workspace/example-project");
        std::fs::create_dir_all(&project).expect("create a project");
        let authorize =
            |root: &Path| Workspace::authorize_beside(root, &directory, Ok(home.clone()));

        let workspace = authorize(&project).expect("a project inside the home directory");
        assert_eq!(workspace.root(), project);
        authorize(&home.join("Workspace")).expect("a directory inside the home directory");
        for broad in [&home, &base, Path::new("/")] {
            assert!(
                matches!(authorize(broad), Err(WorkspaceError::TooBroad)),
                "{}",
                broad.display()
            );
        }
        // A link to the home directory is the home directory.
        symlink(&home, base.join("to-home")).expect("link");
        assert!(matches!(
            authorize(&base.join("to-home")),
            Err(WorkspaceError::TooBroad)
        ));
        // The data directory stays apart, wherever the home directory is.
        assert!(matches!(
            authorize(directory.root()),
            Err(WorkspaceError::OverlapsDataDirectory)
        ));
        assert!(matches!(
            authorize(directory.logs_path()),
            Err(WorkspaceError::OverlapsDataDirectory)
        ));

        // No home directory, no Workspace: not even an ordinary one.
        for unknown in [
            HomeError::NoAccount,
            HomeError::NotAbsolute,
            HomeError::Lookup(std::io::ErrorKind::Other.into()),
            HomeError::Unresolved(std::io::ErrorKind::NotFound.into()),
        ] {
            assert!(matches!(
                Workspace::authorize_beside(&project, &directory, Err(unknown)),
                Err(WorkspaceError::HomeUnknown(_))
            ));
        }
    }

    #[test]
    fn the_accounts_home_directory_and_its_ancestors_are_refused() {
        let (_guard, root, _data, directory) = workspace("account-home");
        let home = account_home().expect("the effective user's home directory");
        assert!(home.is_absolute() && home.is_dir(), "{}", home.display());
        assert_eq!(home.canonicalize().expect("resolve"), home, "resolved");
        for ancestor in home.ancestors() {
            assert!(
                matches!(
                    Workspace::authorize(ancestor, &directory),
                    Err(WorkspaceError::TooBroad)
                ),
                "{}",
                ancestor.display()
            );
        }
        Workspace::authorize(&root, &directory).expect("an ordinary root");
        // This checkout, when it lies inside the home directory, is the
        // kind of project that stays authorized.
        let manifest = Path::new(env!("CARGO_MANIFEST_DIR"))
            .canonicalize()
            .expect("resolve the checkout");
        if manifest.starts_with(&home) && manifest != home {
            Workspace::authorize(&manifest, &directory).expect("a project in the home directory");
        }
    }

    /// Set only in the child processes of
    /// [`home_is_never_taken_from_the_environment`]: the home directory the
    /// parent resolved, and a directory its `HOME` may name.
    const PROBE_HOME: &str = "OCG_TEST_ACCOUNT_HOME";
    const PROBE_OTHER: &str = "OCG_TEST_OTHER_DIRECTORY";

    /// Runs this test again in a child process for each `HOME` an attacker
    /// or a careless launcher could give OCG: none, a missing directory, an
    /// unrelated existing one and a relative path. Each child resolves the
    /// same home directory as this process, still refuses it, and treats
    /// the directory `HOME` names as any other. The environment of this
    /// process is never changed.
    #[test]
    fn home_is_never_taken_from_the_environment() {
        if let Some(expected) = std::env::var_os(PROBE_HOME) {
            let home = account_home().expect("the effective user's home directory");
            assert_eq!(home.as_os_str(), expected, "HOME changed nothing");
            let (_guard, root, _data, directory) = workspace("probe");
            assert!(matches!(
                Workspace::authorize(&home, &directory),
                Err(WorkspaceError::TooBroad)
            ));
            if let Some(parent) = home.parent() {
                assert!(matches!(
                    Workspace::authorize(parent, &directory),
                    Err(WorkspaceError::TooBroad)
                ));
            }
            Workspace::authorize(&root, &directory).expect("an ordinary root");
            let other = std::env::var_os(PROBE_OTHER).expect("the other directory");
            Workspace::authorize(Path::new(&other), &directory)
                .expect("the directory HOME names is no home directory");
            return;
        }
        let home = account_home().expect("the effective user's home directory");
        let (_guard, other) = temporary_directory(SUITE, "home-environment");
        let missing = other.join("missing");
        let name = "orchestration::workspace_read::tests::home_is_never_taken_from_the_environment";
        let environments = [
            None,
            Some(missing.into_os_string()),
            Some(other.clone().into_os_string()),
            Some("relative/home".into()),
        ];
        for claimed in environments {
            let mut child =
                std::process::Command::new(std::env::current_exe().expect("this test binary"));
            child
                .args([name, "--exact", "--test-threads=1", "--nocapture"])
                .env(PROBE_HOME, &home)
                .env(PROBE_OTHER, &other);
            match &claimed {
                Some(value) => child.env("HOME", value),
                None => child.env_remove("HOME"),
            };
            let output = child.output().expect("run the child");
            let stdout = String::from_utf8_lossy(&output.stdout);
            assert!(
                output.status.success() && stdout.contains("1 passed"),
                "HOME={claimed:?}: {stdout}\n{}",
                String::from_utf8_lossy(&output.stderr)
            );
        }
    }

    #[test]
    fn a_replaced_workspace_root_is_refused() {
        let (_guard, root, _data, directory) = workspace("replaced");
        write(&root, "file.txt", b"x");
        let workspace = Workspace::authorize(&root, &directory).expect("authorize");
        // The original directory still exists, so its replacement is another.
        let (_moved_guard, moved) = temporary_directory(SUITE, "replaced-original");
        std::fs::remove_dir(&moved).expect("free the name");
        std::fs::rename(&root, &moved).expect("move the Workspace away");
        std::fs::create_dir(&root).expect("create another directory");
        write(&root, "file.txt", b"y");
        assert_eq!(read(&workspace, "file.txt"), Err(Refusal::WorkspaceChanged));
    }

    #[test]
    fn the_tool_is_registered_only_when_asked_with_its_strict_schema() {
        let (_guard, root, _data, directory) = workspace("registration");
        assert!(
            matches!(
                ToolRegistry::default().offer(&[TOOL_NAME]),
                Err(ToolRegistryError::Unregistered(_))
            ),
            "no tool is registered by default"
        );
        let registry = ToolRegistry::default();
        register(
            &registry,
            Workspace::authorize(&root, &directory).expect("authorize"),
        )
        .expect("register");
        assert!(
            matches!(
                registry.offer(&[TOOL_NAME]),
                Err(ToolRegistryError::Unauthorized(_))
            ),
            "registering authorizes nothing"
        );

        registry.authorize(TOOL_NAME).expect("authorize");
        let offer = registry.offer(&[TOOL_NAME]).expect("offer");
        let wires = offer.wires().expect("offered");
        let [wire] = wires.as_slice() else {
            panic!("exactly one tool is offered: {wires:?}");
        };
        assert_eq!(wire.function.name, TOOL_NAME);
        assert_eq!(wire.function.parameters, Some(parameters()));
        let handler = ReadFile {
            workspace: Workspace::authorize(&root, &directory).expect("authorize"),
        };
        assert_eq!(handler.effect(), ToolEffect::ReadOnly);
        assert!(crate::persistence::is_handler_version(handler.version()));
    }

    #[test]
    fn binary_files_are_read_whole_within_their_limit_and_the_same_boundary() {
        let (_guard, root, _data, directory) = workspace("binary");
        let (_outside_guard, outside, _outside_data, _) = workspace("binary-outside");
        let image = b"\x89PNG\r\n\x1a\n\x00\xff\xfe binary".to_vec();
        write(&root, "images/shot.png", &image);
        write(&root, "big.png", &[0; 64]);
        write(&root, ".hidden.png", &image);
        write(&outside, "secret.png", &image);
        symlink(outside.join("secret.png"), root.join("linked.png")).expect("link");
        symlink(&outside, root.join("outside")).expect("link");
        std::fs::hard_link(root.join("big.png"), root.join("twice.png")).expect("hard link");
        let workspace = Workspace::authorize(&root, &directory).expect("authorize");

        assert_eq!(
            workspace.read_binary("images/shot.png", 1024),
            Ok(image.clone())
        );
        assert_eq!(
            workspace.read_binary("images/shot.png", image.len()),
            Ok(image.clone()),
            "a file at the limit is read"
        );
        assert_eq!(
            workspace.read_binary("images/shot.png", image.len() - 1),
            Err(Refusal::TooLarge)
        );
        let absolute = outside.join("secret.png");
        for (path, refusal) in [
            ("missing.png", Refusal::NotFound),
            ("images", Refusal::NotRegularFile),
            ("linked.png", Refusal::SymbolicLink),
            ("outside/secret.png", Refusal::SymbolicLink),
            (
                "../binary-outside-workspace/secret.png",
                Refusal::InvalidPath,
            ),
            (absolute.to_str().expect("UTF-8"), Refusal::InvalidPath),
            (".hidden.png", Refusal::Protected),
            ("twice.png", Refusal::HardLinked),
        ] {
            assert_eq!(workspace.read_binary(path, 1024), Err(refusal), "{path}");
        }
    }

    /// A ranged call's result, or its refusal's message.
    fn ranged(workspace: &Workspace, arguments: Value) -> Result<Value, String> {
        call(workspace.clone(), arguments).map_err(|failure| {
            assert_eq!(
                failure.certainty,
                crate::orchestration::tools::FailureCertainty::NotApplied
            );
            failure.message
        })
    }

    #[test]
    fn a_file_larger_than_the_whole_file_limit_is_read_exactly_in_ranges() {
        let (_guard, root, _data, directory) = workspace("ranges");
        let text = (1..=3_000)
            .map(|line| format!("line {line}: {}\n", "x".repeat(line % 17)))
            .collect::<String>();
        assert!(text.len() > 4 * MAX_FILE_BYTES);
        write(&root, "src/big.rs", text.as_bytes());
        let workspace = Workspace::authorize(&root, &directory).expect("authorize");
        assert_eq!(
            read(&workspace, "src/big.rs"),
            Err(Refusal::TooLarge),
            "a call without a range reads whole files only, as it always did"
        );

        let mut joined = String::new();
        let mut start = 1;
        let mut calls = 0;
        loop {
            calls += 1;
            assert!(calls < 100, "the traversal ends");
            let result = ranged(
                &workspace,
                json!({"path": "src/big.rs", "start_line": start}),
            )
            .expect("a range");
            assert!(
                result.to_string().len() <= MAX_FILE_BYTES + 256,
                "each result stays bounded"
            );
            let content = result["content"].as_str().expect("text");
            let end = result["end_line"].as_u64().expect("lines were returned");
            assert_eq!(result["start_line"], start);
            assert_eq!(result["total_lines"], 3_000);
            assert_eq!(result["line_truncated"], false);
            assert_eq!(
                content.lines().count() as u64,
                end - start + 1,
                "end_line names the last line returned"
            );
            assert!(content.ends_with('\n'), "only whole lines are returned");
            joined.push_str(content);
            match result["next_start_line"].as_u64() {
                Some(next) => {
                    assert_eq!(next, end + 1);
                    start = next;
                }
                None => {
                    assert_eq!(end, 3_000);
                    break;
                }
            }
        }
        assert_eq!(joined, text, "the ranges are the file, exactly");
        assert!(calls > 4);

        // A narrow range is exactly the lines asked for.
        let result = ranged(
            &workspace,
            json!({"path": "src/big.rs", "start_line": 1_000, "line_count": 3}),
        )
        .expect("a range");
        let expected = text
            .lines()
            .skip(999)
            .take(3)
            .collect::<Vec<_>>()
            .join("\n")
            + "\n";
        assert_eq!(
            result,
            json!({
                "path": "src/big.rs",
                "start_line": 1_000,
                "end_line": 1_002,
                "total_lines": 3_000,
                "next_start_line": 1_003,
                "line_truncated": false,
                "content": expected,
            })
        );
    }

    #[test]
    fn ranges_keep_line_ends_and_report_where_the_file_ends() {
        let (_guard, root, _data, directory) = workspace("range-edges");
        let text = "first\r\nsecond ünïcødé ✓\n\n\"quoted\"\tand \\ last";
        write(&root, "mixed.txt", text.as_bytes());
        write(&root, "empty.txt", b"");
        write(&root, "ended.txt", b"a\nb\n");
        let workspace = Workspace::authorize(&root, &directory).expect("authorize");
        let range = |path: &str, start: u64, count: u64| {
            ranged(
                &workspace,
                json!({"path": path, "start_line": start, "line_count": count}),
            )
            .expect("a range")
        };

        let result = range("mixed.txt", 1, 2);
        assert_eq!(result["content"], "first\r\nsecond ünïcødé ✓\n");
        assert_eq!(
            (&result["end_line"], &result["next_start_line"]),
            (&json!(2), &json!(3))
        );
        let result = range("mixed.txt", 3, 10);
        assert_eq!(result["content"], "\n\"quoted\"\tand \\ last");
        assert_eq!(result["end_line"], 4, "a last line without its end counts");
        assert_eq!(result["total_lines"], 4);
        assert_eq!(result["next_start_line"], Value::Null);

        // Past the end: no line, and nothing to continue from.
        for (path, start, total) in [
            ("mixed.txt", 5, 4),
            ("ended.txt", 3, 2),
            ("empty.txt", 1, 0),
        ] {
            let result = range(path, start, 1);
            assert_eq!(
                result,
                json!({
                    "path": path,
                    "start_line": start,
                    "end_line": null,
                    "total_lines": total,
                    "next_start_line": null,
                    "line_truncated": false,
                    "content": "",
                }),
                "{path}"
            );
        }
        // A range with only one of its arguments defaults the other.
        let result =
            ranged(&workspace, json!({"path": "ended.txt", "line_count": 1})).expect("a range");
        assert_eq!(
            (&result["content"], &result["next_start_line"]),
            (&json!("a\n"), &json!(2))
        );
        let result =
            ranged(&workspace, json!({"path": "ended.txt", "start_line": 2})).expect("a range");
        assert_eq!(
            (&result["content"], &result["end_line"]),
            (&json!("b\n"), &json!(2))
        );
        // Without a range, the result is what it always was.
        assert_eq!(
            call(workspace, json!({"path": "ended.txt"})),
            Ok(json!({"path": "ended.txt", "content": "a\nb\n"}))
        );
    }

    #[test]
    fn a_line_longer_than_the_limit_is_returned_cut_at_a_character_and_marked() {
        let (_guard, root, _data, directory) = workspace("range-long-line");
        let long = format!("a{}", "é".repeat(MAX_FILE_BYTES));
        write(
            &root,
            "minified.js",
            format!("{long}\nshort\n{}\n", "\"".repeat(MAX_FILE_BYTES)).as_bytes(),
        );
        let workspace = Workspace::authorize(&root, &directory).expect("authorize");
        let result =
            ranged(&workspace, json!({"path": "minified.js", "start_line": 1})).expect("a range");
        let content = result["content"].as_str().expect("text");
        assert_eq!(
            content,
            format!("a{}", "é".repeat(MAX_FILE_BYTES / 2 - 1)),
            "the limit falls inside a character, which is left out"
        );
        assert_eq!(result["line_truncated"], true);
        assert_eq!(result["end_line"], 1);
        assert_eq!(result["next_start_line"], 2);

        // A line that would not fit after another is left for the next read.
        let result =
            ranged(&workspace, json!({"path": "minified.js", "start_line": 2})).expect("a range");
        assert_eq!(result["content"], "short\n");
        assert_eq!(
            (&result["end_line"], &result["next_start_line"]),
            (&json!(2), &json!(3))
        );
        assert_eq!(result["line_truncated"], false);

        // The limit counts what the content costs escaped in JSON.
        let result =
            ranged(&workspace, json!({"path": "minified.js", "start_line": 3})).expect("a range");
        assert_eq!(result["content"], "\"".repeat(MAX_FILE_BYTES / 2));
        assert_eq!(result["line_truncated"], true);
        assert!(
            serde_json::to_string(&result["content"])
                .expect("JSON")
                .len()
                <= MAX_FILE_BYTES + 2
        );
    }

    #[test]
    fn invalid_ranges_are_refused_by_the_schema_and_the_handler() {
        let (_guard, root, _data, directory) = workspace("range-invalid");
        write(&root, "file.txt", b"a\nb\n");
        let workspace = Workspace::authorize(&root, &directory).expect("authorize");
        let validator = crate::openai_compatible::admission::compile_parameters(&parameters())
            .expect("an enforceable schema");
        for arguments in [
            json!({"path": "file.txt", "start_line": 0}),
            json!({"path": "file.txt", "start_line": -1}),
            json!({"path": "file.txt", "start_line": 1.5}),
            json!({"path": "file.txt", "start_line": "1"}),
            json!({"path": "file.txt", "line_count": 0}),
            json!({"path": "file.txt", "line_count": MAX_RANGE_LINES + 1}),
            json!({"path": "file.txt", "start_line": null}),
        ] {
            assert!(
                !validator.is_valid(&arguments),
                "the schema refuses {arguments}"
            );
            assert_eq!(
                ranged(&workspace, arguments.clone()),
                Err(Refusal::InvalidRange.to_string()),
                "the handler refuses {arguments} too"
            );
        }
        for arguments in [
            json!({"path": "file.txt", "start_line": 1, "offset": 3}),
            json!({"path": "file.txt", "end_line": 3}),
        ] {
            assert!(!validator.is_valid(&arguments), "{arguments}");
        }
        for arguments in [
            json!({"path": "file.txt", "start_line": 1, "line_count": MAX_RANGE_LINES}),
            json!({"path": "file.txt", "start_line": u64::MAX, "line_count": 1}),
        ] {
            assert!(validator.is_valid(&arguments), "{arguments}");
            assert!(ranged(&workspace, arguments).is_ok());
        }
    }

    #[test]
    fn ranged_reads_keep_every_boundary_of_whole_reads() {
        let (_guard, root, _data, directory) = workspace("range-boundary");
        let (_outside_guard, outside, _outside_data, _) = workspace("range-boundary-outside");
        write(&outside, "secret.txt", b"outside\n");
        write(&root, "inside.txt", b"inside\n");
        write(&root, ".env", b"TOKEN=secret\n");
        write(&root, "keys/server.pem", b"secret\n");
        write(&root, "binary.bin", b"text\n\x00\x01\n");
        write(&root, "latin1.txt", b"ok\ncaf\xe9\n");
        symlink(outside.join("secret.txt"), root.join("escape.txt")).expect("link");
        symlink(&outside, root.join("outside")).expect("link");
        std::fs::write(directory.key_path(), b"key material").expect("write a key");
        std::fs::hard_link(directory.key_path(), root.join("notes.txt")).expect("hard link");
        let huge = std::fs::File::create(root.join("huge.log")).expect("create");
        huge.set_len(MAX_RANGED_FILE_BYTES as u64 + 1)
            .expect("a sparse file");
        let workspace = Workspace::authorize(&root, &directory).expect("authorize");
        let range = |path: &str| ranged(&workspace, json!({"path": path, "start_line": 1}));
        for (path, refusal) in [
            ("escape.txt", Refusal::SymbolicLink),
            ("outside/secret.txt", Refusal::SymbolicLink),
            (".env", Refusal::Protected),
            ("keys/server.pem", Refusal::Protected),
            (
                "../range-boundary-outside-workspace/secret.txt",
                Refusal::InvalidPath,
            ),
            ("notes.txt", Refusal::HardLinked),
            ("huge.log", Refusal::TooLargeForRange),
            ("keys", Refusal::NotRegularFile),
            ("missing.txt", Refusal::NotFound),
        ] {
            assert_eq!(range(path), Err(refusal.to_string()), "{path}");
        }
        // Only the lines returned must be text.
        assert_eq!(
            ranged(&workspace, json!({"path": "binary.bin", "line_count": 1}))
                .expect("a text line")["content"],
            "text\n"
        );
        assert_eq!(range("binary.bin"), Err(Refusal::NotText.to_string()));
        assert_eq!(
            ranged(&workspace, json!({"path": "latin1.txt", "start_line": 2})),
            Err(Refusal::NotText.to_string())
        );
        assert!(
            range("latin1.txt").is_err(),
            "a range holding invalid text returns none of it"
        );

        // A replaced root is refused, whatever the new one holds.
        let (_moved_guard, moved) = temporary_directory(SUITE, "range-boundary-moved");
        std::fs::remove_dir(&moved).expect("free the name");
        std::fs::rename(&root, &moved).expect("move the Workspace away");
        std::fs::create_dir(&root).expect("create another directory");
        write(&root, "inside.txt", b"replaced\n");
        assert_eq!(
            range("inside.txt"),
            Err(Refusal::WorkspaceChanged.to_string())
        );
    }
}
