//! Sensitive-file classification.
//!
//! The classifier is intentionally conservative: a path that *might* contain a
//! credential is never read, never parsed and never cached. Only its path
//! metadata (size, language) is allowed into the repo map. When in doubt, the
//! engine excludes content rather than risk leaking a secret into an index or
//! a context plan.

use serde::{Deserialize, Serialize};
use std::path::Path;

/// The verdict for one path.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Classification {
    pub sensitive: bool,
    pub reason: Option<String>,
}

impl Classification {
    pub fn safe() -> Self {
        Self {
            sensitive: false,
            reason: None,
        }
    }

    pub fn sensitive(reason: impl Into<String>) -> Self {
        Self {
            sensitive: true,
            reason: Some(reason.into()),
        }
    }
}

/// Basenames that are always sensitive, matched case-insensitively.
const SENSITIVE_NAMES: [&str; 16] = [
    ".envrc",
    ".netrc",
    ".npmrc",
    ".pypirc",
    ".git-credentials",
    ".dockercfg",
    "id_rsa",
    "id_dsa",
    "id_ecdsa",
    "id_ed25519",
    "credentials",
    "credentials.json",
    "secrets",
    "secrets.json",
    "auth.json",
    "token.json",
];

/// Basename prefixes that are sensitive (the `credentials*`, `secrets*`,
/// `auth*`, `token*` families).
const SENSITIVE_PREFIXES: [&str; 5] = ["credential", "secret", "auth", "token", "password"];

/// Extensions that indicate key material or credential bundles.
const SENSITIVE_EXTENSIONS: [&str; 9] = [
    "pem", "key", "p12", "pfx", "jks", "keystore", "ppk", "der", "crt",
];

/// Path fragments for known credential stores, including OpenCode's own.
const SENSITIVE_PATH_FRAGMENTS: [&str; 10] = [
    "/.ssh/",
    "/.aws/",
    "/.gnupg/",
    "/.kube/",
    "/.docker/",
    "/.config/opencode/",
    "/.local/share/opencode/",
    "library/application support/opencode/",
    "appdata/roaming/opencode/",
    "/opencode/auth.json",
];

/// Classify a path relative to a repository root. `path` may be relative or
/// absolute; only the textual components are inspected.
pub fn classify(path: &Path) -> Classification {
    let text = path.to_string_lossy().replace('\\', "/");
    let lower = text.to_ascii_lowercase();

    for fragment in SENSITIVE_PATH_FRAGMENTS {
        if lower.contains(fragment) {
            return Classification::sensitive(format!("credential store path ({fragment})"));
        }
    }

    let name = lower.rsplit('/').next().unwrap_or(&lower);
    if name == ".env" || name.starts_with(".env.") {
        return Classification::sensitive("dotenv file");
    }
    for candidate in SENSITIVE_NAMES {
        if name == candidate {
            return Classification::sensitive(format!("sensitive file name ({candidate})"));
        }
    }
    for prefix in SENSITIVE_PREFIXES {
        if name.starts_with(prefix) {
            return Classification::sensitive(format!("sensitive name prefix ({prefix}*)"));
        }
    }
    if let Some(extension) = name.rsplit_once('.').map(|(_, extension)| extension) {
        if SENSITIVE_EXTENSIONS.contains(&extension) {
            return Classification::sensitive(format!("sensitive extension (.{extension})"));
        }
    }
    Classification::safe()
}
