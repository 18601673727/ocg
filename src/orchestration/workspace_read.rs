//! `workspace_read_file`: OCG's one filesystem tool. It reads one small UTF-8
//! text file inside a Workspace the operator authorized, and nothing else.
//!
//! The Workspace root is chosen by the operator, never by a Provider: a call
//! carries only a path relative to it. The root is resolved and checked once,
//! by [`Workspace::authorize`], before the tool can be registered, and it is
//! refused when it is not an existing directory, when it would contain the
//! home directory or the OCG data directory, or when it lies inside the data
//! directory.
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
use crate::orchestration::execution_runtime::{ToolExecution, ToolLoopLimits};
use crate::orchestration::tools::{
    ToolFailure, ToolFuture, ToolHandler, ToolRegistry, ToolRegistryError,
};
use crate::persistence::ToolEffect;

/// The name the tool is offered and recorded under.
pub const TOOL_NAME: &str = "workspace_read_file";

/// The largest file the tool reads. A larger one is refused, never truncated.
pub const MAX_FILE_BYTES: usize = 8 * 1024;

/// Identifies this handler's behavior in every call it records.
const HANDLER_VERSION: &str = "workspace_read_file-1";

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
    #[error("the Workspace root overlaps the OCG data directory")]
    OverlapsDataDirectory,
}

/// A Workspace root the operator authorized for Provider access, resolved
/// and checked. Only [`Workspace::authorize`] makes one.
#[derive(Debug)]
pub struct Workspace {
    root: PathBuf,
    identity: Identity,
    data_directory: Identity,
}

impl Workspace {
    /// Resolves `root` and checks that it may be read from: it must be an
    /// absolute path to an existing directory that neither contains the home
    /// directory nor overlaps `data_directory`.
    ///
    /// # Errors
    ///
    /// Returns why the root is refused.
    pub fn authorize(root: &Path, data_directory: &DataDirectory) -> Result<Self, WorkspaceError> {
        if !root.is_absolute() {
            return Err(WorkspaceError::NotAbsolute);
        }
        let root = root.canonicalize().map_err(WorkspaceError::Unresolved)?;
        let directory = open_directory(&root).map_err(WorkspaceError::Unresolved)?;
        let metadata = directory.metadata().map_err(WorkspaceError::Unresolved)?;
        if !metadata.is_dir() {
            return Err(WorkspaceError::NotDirectory);
        }
        let home = std::env::var_os("HOME")
            .map(PathBuf::from)
            .and_then(|home| home.canonicalize().ok());
        if root.parent().is_none() || home.is_some_and(|home| home.starts_with(&root)) {
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
        let components = components(path)?;
        let (name, parents) = components.split_last().ok_or(Refusal::InvalidPath)?;
        let mut directory = open_directory(&self.root).map_err(|_| Refusal::WorkspaceChanged)?;
        let metadata = directory
            .metadata()
            .map_err(|_| Refusal::WorkspaceChanged)?;
        if Identity::of(&metadata) != self.identity {
            return Err(Refusal::WorkspaceChanged);
        }
        for parent in parents {
            directory = open_at(&directory, parent, Step::Directory)?;
            let metadata = directory.metadata().map_err(|_| Refusal::Unreadable)?;
            if Identity::of(&metadata) == self.data_directory {
                return Err(Refusal::Protected);
            }
        }
        let file = open_at(&directory, name, Step::File)?;
        read_whole(&file)
    }
}

/// Why a call reads nothing. Its message is recorded and may reach the
/// Provider, so it never repeats the path or anything read.
#[derive(Clone, Copy, Debug, PartialEq, Eq, thiserror::Error)]
enum Refusal {
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
    #[error("the file is not UTF-8 text")]
    NotText,
    #[error("the file changed while it was read")]
    Changed,
    #[error("the file could not be read")]
    Unreadable,
}

impl Refusal {
    fn from_errno(errno: Errno) -> Self {
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
fn components(path: &str) -> Result<Vec<&str>, Refusal> {
    if path.is_empty() || path.len() > MAX_PATH_BYTES {
        return Err(Refusal::InvalidPath);
    }
    let components = path.split('/').collect::<Vec<_>>();
    if components.len() > MAX_PATH_COMPONENTS {
        return Err(Refusal::InvalidPath);
    }
    for component in &components {
        if component.is_empty()
            || *component == "."
            || *component == ".."
            || component
                .chars()
                .any(|character| matches!(character, '\\' | ':') || character.is_control())
        {
            return Err(Refusal::InvalidPath);
        }
        if is_protected(component) {
            return Err(Refusal::Protected);
        }
    }
    Ok(components)
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
enum Step {
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
fn open_at(directory: &File, name: &str, step: Step) -> Result<File, Refusal> {
    let stat = rustix::fs::statat(directory, name, AtFlags::SYMLINK_NOFOLLOW)
        .map_err(Refusal::from_errno)?;
    match (FileType::from_raw_mode(stat.st_mode), step) {
        (FileType::Symlink, _) => return Err(Refusal::SymbolicLink),
        (FileType::Directory, Step::Directory) => {}
        (FileType::RegularFile, Step::File) => {
            if !u64::try_from(stat.st_size).is_ok_and(|size| size <= MAX_FILE_BYTES as u64) {
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
fn read_whole(file: &File) -> Result<String, Refusal> {
    let before = file.metadata().map_err(|_| Refusal::Unreadable)?;
    if !before.is_file() {
        return Err(Refusal::NotRegularFile);
    }
    if before.nlink() != 1 {
        return Err(Refusal::HardLinked);
    }
    if before.len() > MAX_FILE_BYTES as u64 {
        return Err(Refusal::TooLarge);
    }
    let mut content = Vec::with_capacity(MAX_FILE_BYTES + 1);
    file.take(MAX_FILE_BYTES as u64 + 1)
        .read_to_end(&mut content)
        .map_err(|_| Refusal::Unreadable)?;
    if content.len() > MAX_FILE_BYTES {
        return Err(Refusal::TooLarge);
    }
    let after = file.metadata().map_err(|_| Refusal::Unreadable)?;
    let unchanged = |metadata: &std::fs::Metadata| {
        (
            metadata.len(),
            metadata.mtime(),
            metadata.mtime_nsec(),
            metadata.ctime(),
            metadata.ctime_nsec(),
        )
    };
    if content.len() as u64 != before.len() || unchanged(&before) != unchanged(&after) {
        return Err(Refusal::Changed);
    }
    let text = String::from_utf8(content).map_err(|_| Refusal::NotText)?;
    if text.contains('\0') {
        return Err(Refusal::NotText);
    }
    Ok(text)
}

/// The parameters a call must match exactly: one relative path.
fn parameters() -> Value {
    json!({
        "type": "object",
        "properties": {
            "path": {
                "type": "string",
                "description": "A path relative to the authorized Workspace root"
            }
        },
        "required": ["path"],
        "additionalProperties": false
    })
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

    /// Reads the file synchronously: it is small and local, so the call
    /// does not yield while it reads, and its time limit cannot interrupt a
    /// read that blocks.
    fn call(&self, arguments: Value) -> ToolFuture<'_> {
        Box::pin(async move {
            let Some(path) = arguments.get("path").and_then(Value::as_str) else {
                return Err(ToolFailure::not_applied(Refusal::InvalidPath.to_string()));
            };
            let content = self
                .workspace
                .read(path)
                .map_err(|refusal| ToolFailure::not_applied(refusal.to_string()))?;
            Ok(json!({"path": path, "content": content}))
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
            "Read the complete contents of one UTF-8 text file, of at most 8 KiB, from the authorized Workspace.",
        ),
        parameters(),
        Arc::new(ReadFile { workspace }),
    )
}

/// The tools of an execution that may read from `workspace`: exactly
/// `workspace_read_file`, registered in a registry of its own and
/// authorized there, with the default [`ToolLoopLimits`]. The registry is
/// not shared, so nothing else can be offered or authorized through it.
///
/// # Errors
///
/// Returns a registration failure, which this tool's fixed definition never
/// causes.
pub fn read_file_tools(workspace: Workspace) -> Result<ToolExecution, ToolRegistryError> {
    let registry = ToolRegistry::default();
    register(&registry, workspace)?;
    registry.authorize(TOOL_NAME)?;
    Ok(ToolExecution {
        offer: registry.offer(&[TOOL_NAME])?,
        limits: ToolLoopLimits::default(),
    })
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

        let tools = read_file_tools(Workspace::authorize(&root, &directory).expect("authorize"))
            .expect("enable the tool");
        let wires = tools.offer.wires().expect("offered");
        let [wire] = wires.as_slice() else {
            panic!("exactly one tool is offered: {wires:?}");
        };
        assert_eq!(wire.function.name, TOOL_NAME);
        assert_eq!(wire.function.parameters, Some(parameters()));
        assert_eq!(tools.limits, ToolLoopLimits::default());
        let handler = ReadFile {
            workspace: Workspace::authorize(&root, &directory).expect("authorize"),
        };
        assert_eq!(handler.effect(), ToolEffect::ReadOnly);
        assert!(crate::persistence::is_handler_version(handler.version()));
    }
}
