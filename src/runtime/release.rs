//! GitHub release metadata.
//!
//! The standalone OpenCode release source is `anomalyco/opencode`; OCG
//! releases come from this repository. Both use the same GitHub release API
//! shape, so one parser handles them.

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

/// Fetch a release for an exact version, trying the `v`-prefixed tag first.
pub fn fetch_release_by_version(
    http: &dyn HttpTransport,
    api_base: &str,
    repo: &str,
    version: &Version,
) -> Result<Release> {
    let mut last_error = None;
    for tag in [format!("v{version}"), version.to_string()] {
        let url = tag_release_url(api_base, repo, &tag);
        match http.get_text(&url) {
            Ok(text) => match parse_release(&text) {
                Ok(release) if release.version == *version => return Ok(release),
                Ok(release) => {
                    last_error = Some(OcgError::config(format!(
                        "release tag '{}' resolved to {}, expected {version}",
                        release.tag, release.version
                    )))
                }
                Err(error) => last_error = Some(error),
            },
            Err(error) => last_error = Some(error),
        }
    }
    Err(last_error.unwrap_or_else(|| {
        OcgError::config(format!("release for OpenCode {version} was not found"))
    }))
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

/// Extract a version from `opencode --version` output.
///
/// Accepts plain `1.18.31`, a `v` prefix, and decorated output such as
/// `opencode 1.18.31`.
pub fn parse_version_output(output: &str) -> Option<Version> {
    for token in output.split_whitespace() {
        let token = token
            .trim_matches(|c: char| !c.is_ascii_alphanumeric() && c != '.' && c != '-' && c != '+');
        if let Some(version) = parse_exact_version(token) {
            return Some(version);
        }
    }
    None
}

/// Parse a tag or pin string into an exact semver.
pub fn parse_exact_version(raw: &str) -> Option<Version> {
    super::policy::parse_exact_version(raw)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_a_github_release_document() {
        let text = r#"{
            "tag_name": "v1.18.31",
            "assets": [
                {"name": "opencode-linux-x64.tar.gz",
                 "browser_download_url": "https://example.test/linux.tar.gz",
                 "digest": "sha256:ABCDEF"},
                {"name": "opencode-darwin-arm64.zip",
                 "browser_download_url": "https://example.test/mac.zip"}
            ]
        }"#;
        let release = parse_release(text).unwrap();
        assert_eq!(release.tag, "v1.18.31");
        assert_eq!(release.version, Version::new(1, 18, 31));
        assert_eq!(
            release.asset("opencode-linux-x64.tar.gz").unwrap().sha256,
            Some("abcdef".to_string())
        );
        assert!(release.asset("missing").is_none());
    }

    #[test]
    fn rejects_a_release_without_a_semver_tag() {
        assert!(parse_release(r#"{"tag_name": "nightly"}"#).is_err());
        assert!(parse_release(r#"{}"#).is_err());
    }

    #[test]
    fn extracts_versions_from_version_output() {
        assert_eq!(
            parse_version_output("1.18.31\n"),
            Some(Version::new(1, 18, 31))
        );
        assert_eq!(
            parse_version_output("opencode v1.18.31"),
            Some(Version::new(1, 18, 31))
        );
        assert_eq!(parse_version_output("no version here"), None);
    }

    #[test]
    fn builds_the_api_url() {
        assert_eq!(
            latest_release_url("https://api.github.com/", "anomalyco/opencode"),
            "https://api.github.com/repos/anomalyco/opencode/releases/latest"
        );
    }

    #[test]
    fn exact_version_fetch_rejects_a_mismatched_release() {
        let requested = Version::new(1, 18, 31);
        let http = crate::http::MemoryHttp::new()
            .with(
                "https://api.test/repos/anomalyco/opencode/releases/tags/v1.18.31",
                br#"{"tag_name":"v1.19.0","assets":[]}"#.to_vec(),
            )
            .with(
                "https://api.test/repos/anomalyco/opencode/releases/tags/1.18.31",
                br#"{"tag_name":"v1.19.0","assets":[]}"#.to_vec(),
            );
        let error =
            fetch_release_by_version(&http, "https://api.test", "anomalyco/opencode", &requested)
                .expect_err("mismatched release must fail");
        assert!(error.to_string().contains("expected 1.18.31"));
    }
}
