//! OCG self-update.
//!
//! Downloads the platform OCG binary plus `SHA256SUMS` from this
//! repository's GitHub releases, verifies the checksum, and atomically
//! replaces the running executable. If anything fails, the installed CLI is
//! left untouched.

use crate::error::{OcgError, Result};
use crate::http::HttpTransport;
use crate::platform::Platform;
use crate::process::ProcessHost;
use semver::Version;
use std::fs;
use std::io::Write;
use std::path::{Path, PathBuf};

/// The result of a self-update attempt.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SelfUpdateOutcome {
    pub from: Version,
    pub to: Version,
    /// False when the installed version already matches the latest release.
    pub updated: bool,
    pub path: PathBuf,
}

/// Download, verify and atomically replace `current_exe`.
///
/// Before the rename, the staged binary is executed through [`ProcessHost`]
/// and its reported version must match the release; otherwise the target is
/// left untouched.
#[allow(clippy::too_many_arguments)]
pub fn self_update(
    http: &dyn HttpTransport,
    api_base: &str,
    repo: &str,
    platform: Platform,
    current_exe: &Path,
    current_version: &Version,
    process: &dyn ProcessHost,
) -> Result<SelfUpdateOutcome> {
    let release = fetch_latest_release(http, api_base, repo)?;
    if release.version <= *current_version {
        return Ok(SelfUpdateOutcome {
            from: current_version.clone(),
            to: release.version,
            updated: false,
            path: current_exe.to_path_buf(),
        });
    }

    let binary = expected_asset(&release, platform.ocg_artifact())?;
    let expected = expected_checksum(http, &release, platform.ocg_artifact())?;
    let bytes = http
        .get(&binary.url)
        .map_err(|error| OcgError::config(format!("cannot download {}: {error}", binary.name)))?;
    crate::hash::verify_sha256(&bytes, &expected, &binary.name)?;

    replace_executable(current_exe, &bytes, &release.version, process)?;

    Ok(SelfUpdateOutcome {
        from: current_version.clone(),
        to: release.version,
        updated: true,
        path: current_exe.to_path_buf(),
    })
}

fn expected_asset<'a>(
    release: &'a crate::release::Release,
    name: &str,
) -> Result<&'a crate::release::ReleaseAsset> {
    release
        .asset(name)
        .ok_or_else(|| OcgError::config(format!("release {} has no asset '{name}'", release.tag)))
}

fn expected_checksum(
    http: &dyn HttpTransport,
    release: &crate::release::Release,
    name: &str,
) -> Result<String> {
    let sums = release.asset("SHA256SUMS").ok_or_else(|| {
        OcgError::config(format!("release {} has no SHA256SUMS asset", release.tag))
    })?;
    let text = http
        .get_text(&sums.url)
        .map_err(|error| OcgError::config(format!("cannot download SHA256SUMS: {error}")))?;
    crate::hash::checksum_for(&text, name)
        .ok_or_else(|| OcgError::config(format!("SHA256SUMS has no entry for {name}")))
}

/// Write bytes to a temp sibling, validate it, then rename over the target.
fn replace_executable(
    current_exe: &Path,
    bytes: &[u8],
    expected_version: &Version,
    process: &dyn ProcessHost,
) -> Result<()> {
    let parent = current_exe
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."));
    let mut temporary = tempfile::Builder::new()
        .prefix(".ocg-self-")
        .tempfile_in(parent)
        .map_err(|error| {
            OcgError::io(
                format!("cannot stage an update next to {}", current_exe.display()),
                error,
            )
        })?;
    temporary
        .write_all(bytes)
        .map_err(|error| OcgError::write(current_exe, error))?;
    temporary
        .flush()
        .map_err(|error| OcgError::write(current_exe, error))?;
    make_executable(temporary.path())?;

    // Validate the staged binary before replacing the running one.
    let reported = process.version(temporary.path())?;
    let parsed = parse_version_output(&reported).ok_or_else(|| {
        OcgError::config(format!(
            "staged OCG binary reported an unparseable version: {reported:?}"
        ))
    })?;
    if parsed != *expected_version {
        return Err(OcgError::config(format!(
            "staged OCG binary reports {parsed}, expected {expected_version}; keeping the installed CLI"
        )));
    }

    temporary
        .persist(current_exe)
        .map_err(|error| OcgError::write(current_exe, error.error))?;
    Ok(())
}

fn make_executable(path: &Path) -> Result<()> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let metadata = fs::metadata(path).map_err(|error| OcgError::read(path, error))?;
        let mut permissions = metadata.permissions();
        permissions.set_mode(0o755);
        fs::set_permissions(path, permissions).map_err(|error| OcgError::write(path, error))?;
    }
    #[cfg(not(unix))]
    {
        let _ = path;
    }
    Ok(())
}

fn parse_version_output(output: &str) -> Option<Version> {
    for token in output.split_whitespace() {
        let token = token
            .trim_matches(|c: char| !c.is_ascii_alphanumeric() && c != '.' && c != '-' && c != '+');
        if let Some(version) = parse_exact_version(token) {
            return Some(version);
        }
    }
    None
}

fn parse_exact_version(raw: &str) -> Option<Version> {
    let trimmed = raw.trim();
    let trimmed = trimmed.strip_prefix('v').unwrap_or(trimmed);
    Version::parse(trimmed).ok()
}

fn fetch_latest_release(
    http: &dyn HttpTransport,
    api_base: &str,
    repo: &str,
) -> Result<crate::release::Release> {
    let url = crate::release::latest_release_url(api_base, repo);
    let text = http.get_text(&url)?;
    crate::release::parse_release(&text)
}
