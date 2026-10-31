use std::cmp::Ordering;
use std::error::Error;
use std::fs;
use std::path::{Path, PathBuf};

use ntex::client::Client;
use ntex::http::{Uri, header};

use crate::system::http;

type Result<T> = std::result::Result<T, Box<dyn Error>>;
const REINSTALL: &str = "this OCG was not installed with install.sh; reinstall with: curl -fsSL https://github.com/18601673727/ocg/releases/latest/download/install.sh | sh";
const RELEASES: &str = "https://github.com/18601673727/ocg/releases";

#[derive(Clone)]
pub(crate) struct Install {
    pub(crate) prefix: PathBuf,
    pub(crate) port: String,
    pub(crate) token: String,
}

fn setting<'a>(text: &'a str, key: &str) -> Option<&'a str> {
    let prefix = format!("{key}='");
    let mut values = text
        .lines()
        .filter_map(|line| line.strip_prefix(&prefix)?.strip_suffix('\''));
    let value = values.next()?;
    if values.next().is_some() {
        None
    } else {
        Some(value)
    }
}

pub(crate) fn install() -> Result<Install> {
    fn find() -> Option<Install> {
        let exe = fs::canonicalize(std::env::current_exe().ok()?).ok()?;
        let settings = exe.parent()?.parent()?.parent()?.join("ocg.env");
        let text = fs::read_to_string(settings).ok()?;
        let ui = setting(&text, "OCG_UI_DIR")?;
        let prefix = PathBuf::from(ui.strip_suffix("/current/ui")?);
        if !prefix.is_absolute()
            || fs::canonicalize(prefix.join("current/ocg")).ok()? != fs::canonicalize(exe).ok()?
        {
            return None;
        }
        Some(Install {
            prefix,
            port: setting(&text, "OCG_CONTROL_PORT")?.to_owned(),
            token: setting(&text, "OCG_CONTROL_TOKEN")?.to_owned(),
        })
    }
    find().ok_or_else(|| REINSTALL.into())
}

#[derive(Eq, PartialEq)]
pub(crate) struct Version {
    core: [u64; 3],
    pre: Vec<String>,
}
impl Version {
    pub(crate) fn parse(value: &str) -> Option<Self> {
        let (core, pre) = value
            .split_once('-')
            .map_or((value, None), |(a, b)| (a, Some(b)));
        let mut nums = core.split('.');
        let number = |s: &str| {
            if s.is_empty()
                || (s.len() > 1 && s.starts_with('0'))
                || !s.bytes().all(|c| c.is_ascii_digit())
            {
                None
            } else {
                s.parse().ok()
            }
        };
        let core = [
            number(nums.next()?)?,
            number(nums.next()?)?,
            number(nums.next()?)?,
        ];
        if nums.next().is_some() {
            return None;
        }
        let mut parts = Vec::new();
        if let Some(pre) = pre {
            for part in pre.split('.') {
                if part.is_empty()
                    || !part.bytes().all(|c| c.is_ascii_alphanumeric() || c == b'-')
                    || (part.len() > 1
                        && part.starts_with('0')
                        && part.bytes().all(|c| c.is_ascii_digit()))
                {
                    return None;
                }
                parts.push(part.to_owned());
            }
        }
        Some(Self { core, pre: parts })
    }
}
impl Ord for Version {
    fn cmp(&self, other: &Self) -> Ordering {
        self.core
            .cmp(&other.core)
            .then_with(|| match (self.pre.is_empty(), other.pre.is_empty()) {
                (true, true) => Ordering::Equal,
                (true, false) => Ordering::Greater,
                (false, true) => Ordering::Less,
                (false, false) => {
                    for (a, b) in self.pre.iter().zip(&other.pre) {
                        let an = a.bytes().all(|c| c.is_ascii_digit());
                        let bn = b.bytes().all(|c| c.is_ascii_digit());
                        let order = match (an, bn) {
                            (true, true) => a.len().cmp(&b.len()).then_with(|| a.cmp(b)),
                            (true, false) => Ordering::Less,
                            (false, true) => Ordering::Greater,
                            (false, false) => a.cmp(b),
                        };
                        if order != Ordering::Equal {
                            return order;
                        }
                    }
                    self.pre.len().cmp(&other.pre.len())
                }
            })
    }
}
impl PartialOrd for Version {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

fn allowed(url: &str, local: bool) -> Result<Uri> {
    let uri: Uri = url.parse()?;
    let scheme = uri.scheme_str().ok_or("release URL has no scheme")?;
    let host = uri.host().ok_or("release URL has no host")?;
    if scheme != "https"
        && !(local && scheme == "http" && matches!(host, "127.0.0.1" | "localhost"))
    {
        return Err("release URL must use HTTPS".into());
    }
    Ok(uri)
}

pub(crate) async fn fetch(client: &Client, url: &str, local: bool) -> Result<Vec<u8>> {
    let mut url = url.to_owned();
    for hop in 0..=5 {
        let uri = allowed(&url, local)?;
        let response = client
            .get(url.as_str())
            .force_close()
            .send()
            .await
            .map_err(|e| format!("release request failed: {e}"))?;
        let status = response.status();
        if status.is_redirection() {
            if hop == 5 {
                return Err("too many release redirects".into());
            }
            let location = response
                .headers()
                .get(header::LOCATION)
                .ok_or("redirect has no Location")?
                .to_str()?;
            url = if location.starts_with("https://") || location.starts_with("http://") {
                location.to_owned()
            } else if location.starts_with('/') {
                format!(
                    "{}://{}{}",
                    uri.scheme_str().ok_or("missing scheme")?,
                    uri.authority().ok_or("missing authority")?,
                    location
                )
            } else {
                format!(
                    "{}/{}",
                    url.rsplit_once('/').ok_or("invalid URL")?.0,
                    location
                )
            };
            continue;
        }
        if !status.is_success() {
            return Err(format!("release request returned {status}").into());
        }
        return response
            .body()
            .await
            .map(|bytes| bytes.to_vec())
            .map_err(|e| format!("release body failed: {e}").into());
    }
    Err("too many release redirects".into())
}

pub(crate) fn release_info(sums: &str) -> Result<(String, String)> {
    let mut version: Option<String> = None;
    let mut installer: Option<String> = None;
    for line in sums.lines() {
        let mut fields = line.split_whitespace();
        let Some(hash) = fields.next() else { continue };
        let Some(name) = fields.next().map(|s| s.trim_start_matches('*')) else {
            continue;
        };
        if name == "install.sh" && installer.replace(hash.to_owned()).is_some() {
            return Err("duplicate installer checksum".into());
        }
        if let Some(name) = name
            .strip_prefix("ocg-v")
            .and_then(|s| s.strip_suffix(".tar.gz"))
        {
            let parts: Vec<&str> = name.rsplitn(4, '-').collect();
            let target_parts = if parts.get(2) == Some(&"unknown") {
                4
            } else {
                3
            };
            let found = name
                .rsplitn(target_parts + 1, '-')
                .nth(target_parts)
                .filter(|found| Version::parse(found).is_some())
                .ok_or("invalid archive name in SHA256SUMS")?;
            if version.as_deref().is_some_and(|v| v != found) {
                return Err("release archives have different versions".into());
            }
            version = Some(found.to_owned());
        }
    }
    Ok((
        version.ok_or("no release archives in SHA256SUMS")?,
        installer.ok_or("missing installer checksum")?,
    ))
}

pub(crate) fn service_file(home: &Path) -> PathBuf {
    if cfg!(target_os = "macos") {
        home.join("Library/LaunchAgents/io.github.18601673727.ocg.plist")
    } else {
        home.join(".config/systemd/user/ocg.service")
    }
}

pub(crate) async fn lookup(
    requested: Option<&str>,
) -> Result<(Client, String, bool, String, String)> {
    let mirror = requested
        .and_then(|_| std::env::var("OCG_RELEASE_URL").ok())
        .filter(|s| !s.is_empty());
    let default_base = format!("{RELEASES}/download");
    let base = match requested {
        Some(version) => {
            let base = mirror.as_deref().unwrap_or(&default_base);
            format!("{}/v{version}", base.strip_suffix('/').unwrap_or(base))
        }
        None => format!("{RELEASES}/latest/download"),
    };
    let local = mirror.as_deref().is_some_and(|base| {
        base.parse::<Uri>().ok().is_some_and(|uri| {
            uri.scheme_str() == Some("http")
                && matches!(uri.host(), Some("127.0.0.1" | "localhost"))
        })
    });
    allowed(&base, local)?;
    let client = http::release_client().await?;
    let sums = fetch(&client, &format!("{base}/SHA256SUMS"), local).await?;
    let (version, checksum) = release_info(std::str::from_utf8(&sums)?)?;
    if requested.is_some_and(|v| v != version) {
        return Err("release version does not match request".into());
    }
    Ok((client, base, local, version, checksum))
}
