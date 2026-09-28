//! Safe managed OpenCode install.
//!
//! A managed runtime lives at
//! `<project>/.ocg/runtime/opencode/<version>/opencode`. Installs are
//! staged in a sibling directory on the same filesystem and moved into place
//! with a single rename, so a crash never leaves a half-installed runtime
//! active. `active.json` is the pointer to the version in use.

use crate::clock::Clock;
use crate::error::{OcgError, Result};
use crate::http::HttpTransport;
use crate::platform::Platform;
use crate::runtime::archive::{extract_opencode, make_executable};
use crate::runtime::hash::verify_sha256;
use crate::runtime::release::Release;
use semver::Version;
use serde_json::{json, Value};
use std::fs;
use std::io::Write;
use std::path::{Path, PathBuf};

const RUNTIME_DIR: &str = ".ocg";
const GITIGNORE_ENTRY: &str = ".ocg/";

/// Paths for the managed runtime of one project.
#[derive(Debug, Clone)]
pub struct Layout {
    project_root: PathBuf,
}

impl Layout {
    pub fn new(project_root: impl Into<PathBuf>) -> Self {
        Self {
            project_root: project_root.into(),
        }
    }

    /// `<project>/.ocg/runtime/opencode`.
    pub fn runtime_root(&self) -> PathBuf {
        self.project_root
            .join(RUNTIME_DIR)
            .join("runtime")
            .join("opencode")
    }

    /// `<runtime>/<version>`.
    pub fn version_dir(&self, version: &Version) -> PathBuf {
        self.runtime_root().join(version.to_string())
    }

    /// `<runtime>/<version>/opencode`.
    pub fn binary_path(&self, version: &Version) -> PathBuf {
        self.version_dir(version).join("opencode")
    }

    /// `<runtime>/active.json`.
    pub fn active_path(&self) -> PathBuf {
        self.runtime_root().join("active.json")
    }
}

/// The pointer to the managed runtime currently in use.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ActiveRuntime {
    pub version: Version,
    pub path: PathBuf,
    pub installed_at: i64,
}

impl ActiveRuntime {
    pub fn read(project_root: &Path) -> Option<Self> {
        let layout = Layout::new(project_root);
        let text = fs::read_to_string(layout.active_path()).ok()?;
        Self::parse(&text, &layout)
    }

    fn parse(text: &str, layout: &Layout) -> Option<Self> {
        let value: Value = serde_json::from_str(text).ok()?;
        let version = value
            .get("version")
            .and_then(Value::as_str)
            .and_then(crate::runtime::policy::parse_exact_version)?;
        let installed_at = value
            .get("installed_at")
            .and_then(Value::as_i64)
            .unwrap_or(0);
        // Never trust an arbitrary recorded path: derive the canonical
        // in-project path from the version and require it to be executable.
        let path = layout.binary_path(&version);
        if !crate::process::is_executable(&path) {
            return None;
        }
        Some(Self {
            version,
            path,
            installed_at,
        })
    }

    /// Write `active.json` atomically.
    pub fn write(&self, project_root: &Path) -> Result<()> {
        let layout = Layout::new(project_root);
        let target = layout.active_path();
        write_json_atomic(
            &target,
            &json!({
                "version": self.version.to_string(),
                "path": self.path.to_string_lossy(),
                "installed_at": self.installed_at,
            }),
        )
    }
}

/// Download, verify and install one exact OpenCode version, then activate it.
///
/// Idempotent: when the exact version is already present the download is
/// skipped and the existing binary is simply re-activated.
pub fn install_opencode(
    project_root: &Path,
    platform: Platform,
    version: &Version,
    release: &Release,
    http: &dyn HttpTransport,
    clock: &dyn Clock,
) -> Result<PathBuf> {
    let layout = Layout::new(project_root);
    let binary = layout.binary_path(version);

    if !crate::process::is_executable(&binary) {
        let asset_name = platform.opencode_asset();
        let asset = release.asset(asset_name).ok_or_else(|| {
            OcgError::config(format!(
                "release {} has no asset '{asset_name}' for {}",
                release.tag,
                platform.slug()
            ))
        })?;

        // Fail closed: an asset without a `sha256:` digest is never installed.
        let expected = asset.sha256.as_ref().ok_or_else(|| {
            OcgError::config(format!(
                "release {} asset '{asset_name}' has no sha256 digest; refusing to install",
                release.tag
            ))
        })?;

        let bytes = http
            .get(&asset.url)
            .map_err(|error| OcgError::config(format!("cannot download {asset_name}: {error}")))?;

        verify_sha256(&bytes, expected, asset_name)?;

        let runtime_root = layout.runtime_root();
        fs::create_dir_all(&runtime_root).map_err(|error| {
            OcgError::io(format!("cannot create {}", runtime_root.display()), error)
        })?;

        let version_dir = layout.version_dir(version);
        if version_dir.exists() {
            // A previous install left an empty or partial directory behind.
            fs::remove_dir_all(&version_dir).map_err(|error| {
                OcgError::io(format!("cannot clear {}", version_dir.display()), error)
            })?;
        }

        let staging = tempfile::Builder::new()
            .prefix(".staging-")
            .tempdir_in(&runtime_root)
            .map_err(|error| {
                OcgError::io(
                    format!("cannot stage an install in {}", runtime_root.display()),
                    error,
                )
            })?;
        let staged_binary = staging.path().join("opencode");
        extract_opencode(platform.archive_kind(), &bytes, &staged_binary)?;
        make_executable(&staged_binary)?;

        // Atomic activation: the staged directory becomes the version dir.
        fs::rename(staging.path(), &version_dir).map_err(|error| {
            OcgError::io(format!("cannot activate {}", version_dir.display()), error)
        })?;
        drop(staging);
    }

    // First managed bootstrap: keep the runtime tree out of the project's VCS.
    ensure_gitignore(project_root)?;

    ActiveRuntime {
        version: version.clone(),
        path: binary.clone(),
        installed_at: clock.now_unix(),
    }
    .write(project_root)?;

    Ok(binary)
}

/// Append `.ocg/` once, without rewriting anything else.
pub fn ensure_gitignore(project_root: &Path) -> Result<()> {
    let path = project_root.join(".gitignore");
    // A missing file is an empty file; any other read error (permissions,
    // non-UTF8) must never be treated as empty and overwritten.
    let existing = match fs::read_to_string(&path) {
        Ok(text) => text,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => String::new(),
        Err(error) => return Err(OcgError::read(&path, error)),
    };
    let already = existing.lines().any(|line| {
        let trimmed = line.trim();
        trimmed == ".ocg/" || trimmed == ".ocg"
    });
    if already {
        return Ok(());
    }
    let mut content = existing;
    if !content.is_empty() && !content.ends_with('\n') {
        content.push('\n');
    }
    content.push_str(GITIGNORE_ENTRY);
    content.push('\n');
    fs::write(&path, content).map_err(|error| OcgError::write(&path, error))
}

/// Write a JSON document atomically (temp sibling + rename).
pub fn write_json_atomic(target: &Path, value: &Value) -> Result<()> {
    if let Some(parent) = target.parent() {
        fs::create_dir_all(parent)
            .map_err(|error| OcgError::io(format!("cannot create {}", parent.display()), error))?;
    }
    let parent = target.parent().unwrap_or_else(|| Path::new("."));
    let mut temporary = tempfile::Builder::new()
        .prefix(".active-")
        .tempfile_in(parent)
        .map_err(|error| OcgError::io(format!("cannot write {}", target.display()), error))?;
    let text = format!("{value}\n");
    temporary
        .write_all(text.as_bytes())
        .map_err(|error| OcgError::write(target, error))?;
    temporary
        .persist(target)
        .map_err(|error| OcgError::write(target, error.error))?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn layout_paths_are_stable() {
        let layout = Layout::new("/tmp/project");
        let version = Version::new(1, 18, 31);
        assert_eq!(
            layout.binary_path(&version),
            PathBuf::from("/tmp/project/.ocg/runtime/opencode/1.18.31/opencode")
        );
        assert_eq!(
            layout.active_path(),
            PathBuf::from("/tmp/project/.ocg/runtime/opencode/active.json")
        );
    }

    #[test]
    fn gitignore_is_created_then_left_alone() {
        let dir = tempfile::tempdir().unwrap();
        ensure_gitignore(dir.path()).unwrap();
        assert_eq!(
            fs::read_to_string(dir.path().join(".gitignore")).unwrap(),
            ".ocg/\n"
        );
        // A second call must not duplicate the entry.
        ensure_gitignore(dir.path()).unwrap();
        assert_eq!(
            fs::read_to_string(dir.path().join(".gitignore")).unwrap(),
            ".ocg/\n"
        );

        let dir2 = tempfile::tempdir().unwrap();
        fs::write(dir2.path().join(".gitignore"), "target/\nnode_modules\n").unwrap();
        ensure_gitignore(dir2.path()).unwrap();
        assert_eq!(
            fs::read_to_string(dir2.path().join(".gitignore")).unwrap(),
            "target/\nnode_modules\n.ocg/\n"
        );
    }

    #[cfg(unix)]
    fn make_executable(path: &Path) {
        use std::os::unix::fs::PermissionsExt;
        let mut permissions = fs::metadata(path).unwrap().permissions();
        permissions.set_mode(0o755);
        fs::set_permissions(path, permissions).unwrap();
    }

    #[cfg(not(unix))]
    fn make_executable(_path: &Path) {}

    #[test]
    fn active_round_trips_and_rejects_missing_paths() {
        let dir = tempfile::tempdir().unwrap();
        let version = Version::new(1, 18, 31);
        let binary = Layout::new(dir.path()).binary_path(&version);
        fs::create_dir_all(binary.parent().unwrap()).unwrap();
        fs::write(&binary, b"x").unwrap();
        make_executable(&binary);
        ActiveRuntime {
            version: version.clone(),
            path: binary.clone(),
            installed_at: 7,
        }
        .write(dir.path())
        .unwrap();
        let active = ActiveRuntime::read(dir.path()).unwrap();
        assert_eq!(active.version, version);
        assert_eq!(active.path, binary);

        fs::remove_file(&binary).unwrap();
        assert!(ActiveRuntime::read(dir.path()).is_none());
    }

    #[test]
    fn active_ignores_an_arbitrary_recorded_path() {
        let dir = tempfile::tempdir().unwrap();
        let version = Version::new(1, 18, 31);
        let canonical = Layout::new(dir.path()).binary_path(&version);
        fs::create_dir_all(canonical.parent().unwrap()).unwrap();
        fs::write(&canonical, b"x").unwrap();
        make_executable(&canonical);
        let outside = dir.path().join("elsewhere/opencode");
        fs::create_dir_all(outside.parent().unwrap()).unwrap();
        fs::write(&outside, b"x").unwrap();
        make_executable(&outside);
        write_json_atomic(
            &Layout::new(dir.path()).active_path(),
            &json!({
                "version": version.to_string(),
                "path": outside.to_string_lossy(),
                "installed_at": 7,
            }),
        )
        .unwrap();
        let active = ActiveRuntime::read(dir.path()).unwrap();
        assert_eq!(active.path, canonical, "must use the canonical path");
    }

    #[test]
    fn active_requires_an_executable_binary() {
        let dir = tempfile::tempdir().unwrap();
        let version = Version::new(1, 18, 31);
        let binary = Layout::new(dir.path()).binary_path(&version);
        fs::create_dir_all(binary.parent().unwrap()).unwrap();
        fs::write(&binary, b"x").unwrap();
        // Deliberately leave it non-executable.
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let mut permissions = fs::metadata(&binary).unwrap().permissions();
            permissions.set_mode(0o644);
            fs::set_permissions(&binary, permissions).unwrap();
        }
        ActiveRuntime {
            version: version.clone(),
            path: binary,
            installed_at: 7,
        }
        .write(dir.path())
        .unwrap();
        #[cfg(unix)]
        assert!(ActiveRuntime::read(dir.path()).is_none());
        #[cfg(not(unix))]
        assert!(ActiveRuntime::read(dir.path()).is_some());
    }

    #[cfg(unix)]
    #[test]
    fn gitignore_refuses_to_overwrite_an_unreadable_file() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join(".gitignore");
        // Invalid UTF-8 makes `read_to_string` fail with a non-NotFound error.
        fs::write(&path, [0xff, 0xfe, 0xfd]).unwrap();
        assert!(ensure_gitignore(dir.path()).is_err());
        assert_eq!(fs::read(&path).unwrap(), vec![0xff, 0xfe, 0xfd]);
    }
}
