//! GitHub release metadata.
//!
//! OCG releases come from this repository. The GitHub release API shape
//! is parsed here.

use crate::error::{OcgError, Result};
use crate::http::HttpTransport;
use semver::Version;
use serde_json::Value;

/// One downloadable release asset.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReleaseAsset {
    pub name: String,
    pub url: String,
    /// `sha256:<hex>` when the API reports a digest.
    pub sha256: Option<String>,
}

/// A parsed release.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Release {
    pub tag: String,
    pub version: Version,
    pub assets: Vec<ReleaseAsset>,
}

impl Release {
    pub fn asset(&self, name: &str) -> Option<&ReleaseAsset> {
        self.assets.iter().find(|asset| asset.name == name)
    }
}

/// The GitHub API endpoint for the latest release of a repository.
pub fn latest_release_url(api_base: &str, repo: &str) -> String {
    format!(
        "{}/repos/{}/releases/latest",
        api_base.trim_end_matches('/'),
        repo
    )
}

/// The GitHub API endpoint for a release tag.
pub fn tag_release_url(api_base: &str, repo: &str, tag: &str) -> String {
    format!(
        "{}/repos/{}/releases/tags/{}",
        api_base.trim_end_matches('/'),
        repo,
        tag
    )
}

/// Fetch and parse the latest release for `repo`.
pub fn fetch_latest_release(
    http: &dyn HttpTransport,
    api_base: &str,
    repo: &str,
) -> Result<Release> {
    let url = latest_release_url(api_base, repo);
    let text = http.get_text(&url)?;
    parse_release(&text)
}

/// Parse a GitHub release document.
pub fn parse_release(text: &str) -> Result<Release> {
    let value: Value = serde_json::from_str(text).map_err(|error| {
        OcgError::config(format!("release metadata is not valid JSON: {error}"))
    })?;
    let tag = value
        .get("tag_name")
        .and_then(Value::as_str)
        .filter(|tag| !tag.is_empty())
        .ok_or_else(|| OcgError::config("release metadata is missing tag_name"))?
        .to_string();
    let version = parse_exact_version(&tag)
        .ok_or_else(|| OcgError::config(format!("release tag '{tag}' is not a semver version")))?;
    let mut assets = Vec::new();
    if let Some(entries) = value.get("assets").and_then(Value::as_array) {
        for entry in entries {
            if let Some(asset) = parse_asset(entry) {
                assets.push(asset);
            }
        }
    }
    Ok(Release {
        tag,
        version,
        assets,
    })
}

fn parse_asset(value: &Value) -> Option<ReleaseAsset> {
    let name = value.get("name").and_then(Value::as_str)?.to_string();
    let url = value
        .get("browser_download_url")
        .and_then(Value::as_str)?
        .to_string();
    let sha256 = value
        .get("digest")
        .and_then(Value::as_str)
        .and_then(|digest| digest.strip_prefix("sha256:"))
        .map(|hex| hex.to_ascii_lowercase());
    Some(ReleaseAsset { name, url, sha256 })
}

/// Parse a tag or pin string into an exact semver.
pub fn parse_exact_version(raw: &str) -> Option<Version> {
    let trimmed = raw.trim();
    let trimmed = trimmed.strip_prefix('v').unwrap_or(trimmed);
    Version::parse(trimmed).ok()
}
