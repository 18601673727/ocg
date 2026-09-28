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

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;

    fn sensitive(path: &str) -> bool {
        classify(&PathBuf::from(path)).sensitive
    }

    #[test]
    fn dotenv_files_are_sensitive() {
        assert!(sensitive(".env"));
        assert!(sensitive("app/.env"));
        assert!(sensitive("app/.env.production"));
        assert!(sensitive("config/.env.local"));
        assert!(sensitive(".envrc"));
        assert!(sensitive("sub/.envrc"));
        assert!(!sensitive("src/environment.rs"));
    }

    #[test]
    fn key_material_is_sensitive() {
        assert!(sensitive("certs/server.pem"));
        assert!(sensitive("keys/private.key"));
        assert!(sensitive("id_rsa"));
        assert!(sensitive("sub/id_ed25519"));
        assert!(sensitive("store/client.p12"));
    }

    #[test]
    fn credential_families_are_sensitive() {
        assert!(sensitive("credentials"));
        assert!(sensitive("credentials.json"));
        assert!(sensitive("src/secrets.rs"));
        assert!(sensitive("src/token_store.rs"));
        assert!(sensitive("auth.json"));
        assert!(sensitive("src/authentication.rs"));
    }

    #[test]
    fn known_credential_locations_are_sensitive() {
        assert!(sensitive(concat!("/", "home/", "someone/.ssh/config")));
        assert!(sensitive(concat!(
            "/",
            "home/",
            "someone/.local/share/opencode/auth.json"
        )));
        assert!(sensitive(concat!(
            "/",
            "home/",
            "someone/.config/opencode/config.json"
        )));
        assert!(sensitive(concat!(
            "/",
            "Users/",
            "someone/Library/Application Support/opencode/auth.json"
        )));
    }

    #[test]
    fn ordinary_source_is_safe() {
        assert!(!sensitive("src/context/engine.rs"));
        assert!(!sensitive("tests/context_tests.rs"));
        assert!(!sensitive("docs/architecture.md"));
        assert!(!sensitive("Cargo.toml"));
        assert!(!sensitive("src/model.rs"));
    }

    #[test]
    fn prefix_families_over_match_conservatively() {
        // The `auth*` family deliberately excludes `author.rs` too: a false
        // positive only costs an index entry, a false negative leaks.
        assert!(sensitive("src/author.rs"));
        assert!(sensitive("src/authentication.rs"));
    }
}
