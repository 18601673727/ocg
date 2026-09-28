//! Repository hygiene: the published tree must not leak secrets, private
//! project tokens or absolute home paths, and every JSON file must parse.

use serde_json::Value;
use std::fs;
use std::path::{Path, PathBuf};

fn repo_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
}

const SKIP_DIRS: [&str; 6] = [
    ".git",
    "target",
    "__pycache__",
    "node_modules",
    ".ocg",
    // Generated frontend build output. It is git-ignored, and the documented
    // verification list runs `pnpm build`, so scanning it would fail on any
    // machine where the local build directory exists.
    ".next",
];

const TEXT_SUFFIXES: [&str; 11] = [
    ".md", ".json", ".rs", ".toml", ".sh", ".txt", ".yml", ".yaml", ".cfg", ".ini", ".lock",
];

fn walk(dir: &Path, files: &mut Vec<PathBuf>) {
    let Ok(entries) = fs::read_dir(dir) else {
        return;
    };
    for entry in entries.flatten() {
        let path = entry.path();
        let name = entry.file_name();
        let name = name.to_string_lossy();
        if path.is_dir() {
            if !SKIP_DIRS.contains(&name.as_ref()) {
                walk(&path, files);
            }
        } else {
            files.push(path);
        }
    }
}

fn is_text_file(path: &Path) -> bool {
    let name = path
        .file_name()
        .map(|name| name.to_string_lossy())
        .unwrap_or_default();
    // The conversation buffer is intentionally git-ignored and must never be
    // committed; it is not part of the published tree, so it is not scanned.
    let buffer = concat!("forward_", "to_gpt.md");
    if name == buffer {
        return false;
    }
    if name == "LICENSE" || name == ".gitignore" {
        return true;
    }
    path.extension()
        .map(|extension| {
            let extension = format!(".{}", extension.to_string_lossy());
            TEXT_SUFFIXES.contains(&extension.as_str())
        })
        .unwrap_or(false)
}

fn text_files() -> Vec<PathBuf> {
    let mut files = Vec::new();
    walk(&repo_root(), &mut files);
    files
        .into_iter()
        .filter(|path| is_text_file(path))
        .collect()
}

#[test]
fn all_json_files_parse() {
    let mut files = Vec::new();
    walk(&repo_root(), &mut files);
    for path in files {
        if path
            .extension()
            .map(|extension| extension == "json")
            .unwrap_or(false)
        {
            let text = fs::read_to_string(&path).unwrap_or_default();
            let parsed: Result<Value, _> = serde_json::from_str(&text);
            assert!(parsed.is_ok(), "invalid JSON: {}", path.display());
        }
    }
}

#[test]
fn no_private_project_tokens() {
    let forward_token = concat!("forward_", "to_gpt");
    let tokens = [
        concat!("Zh", "uju"),
        concat!("Route", "Lace"),
        concat!("Cai", "bao"),
        concat!("xiang", "min"),
        concat!("chun", "cheon"),
        concat!("/opt/", "zhuju"),
        concat!("zj-", "builder"),
        concat!("zj-", "explorer"),
        concat!("zj-", "verifier"),
        concat!("zj-", "docs"),
        forward_token,
    ];
    for path in text_files() {
        let text = fs::read_to_string(&path).unwrap_or_default();
        // The tracked `.gitignore` must be allowed to name the mandated buffer
        // file. Every other protected token, and every production source or
        // doc, is still rejected.
        let is_gitignore = path
            .file_name()
            .map(|name| name == ".gitignore")
            .unwrap_or(false);
        for token in tokens {
            if is_gitignore && token == forward_token {
                continue;
            }
            assert!(
                !text.contains(token),
                "{} contains private token {token}",
                path.display()
            );
        }
    }
}

#[test]
fn no_secrets_or_absolute_home_paths() {
    for path in text_files() {
        let text = fs::read_to_string(&path).unwrap_or_default();
        assert!(
            !contains_aws_key(&text),
            "{} contains an AWS access key shape",
            path.display()
        );
        assert!(
            !contains_openai_key(&text),
            "{} contains an API key shape",
            path.display()
        );
        assert!(
            !contains_github_token(&text),
            "{} contains a GitHub token shape",
            path.display()
        );
        assert!(
            !contains_private_key_header(&text),
            "{} contains a private key header",
            path.display()
        );
        assert!(
            !contains_home_path(&text, "/home/"),
            "{} contains an absolute home path",
            path.display()
        );
        assert!(
            !contains_home_path(&text, "/Users/"),
            "{} contains an absolute home path",
            path.display()
        );
        assert!(
            !contains_email(&text),
            "{} contains an email address",
            path.display()
        );
        assert!(
            !contains_bearer_token(&text),
            "{} contains a Bearer token shape",
            path.display()
        );
        assert!(
            !contains_credential_assignment(&text),
            "{} contains a credential assignment",
            path.display()
        );
        assert!(
            !contains_authorization_header(&text),
            "{} contains an Authorization header value",
            path.display()
        );
    }
}

/// `Bearer <long-token>` anywhere in the text.
fn contains_bearer_token(text: &str) -> bool {
    contains_keyword_run(text, "bearer ", 20, |c| {
        c.is_ascii_alphanumeric() || "._-".contains(c)
    })
}

/// `Authorization: <scheme> <long-token>` in any casing.
fn contains_authorization_header(text: &str) -> bool {
    let lower = text.to_ascii_lowercase();
    let mut search = lower.as_str();
    while let Some(index) = search.find("authorization") {
        let rest =
            search[index + "authorization".len()..].trim_start_matches([' ', '\t', '"', '\'']);
        if let Some(value) = rest.strip_prefix(':') {
            let value = value.trim_start_matches([' ', '\t', '"', '\'']);
            let count = value
                .chars()
                .take_while(|c| c.is_ascii_alphanumeric() || "._-".contains(*c))
                .count();
            if count >= 20 {
                return true;
            }
        }
        search = &search[index + "authorization".len()..];
    }
    false
}

/// `password=`, `api_key=`, `secret=`, ... followed by a long value.
fn contains_credential_assignment(text: &str) -> bool {
    let lower = text.to_ascii_lowercase();
    [
        "password=",
        "password:",
        "passwd=",
        "api_key=",
        "apikey=",
        "api-key=",
        "client_secret=",
        "client-secret=",
        "secret=",
        "access_key=",
        "access-key=",
        "token=",
    ]
    .iter()
    .any(|keyword| {
        contains_keyword_run(&lower, keyword, 12, |c| {
            c.is_ascii_alphanumeric() || "._-+/=~".contains(c)
        })
    })
}

fn contains_keyword_run(
    text: &str,
    marker: &str,
    minimum: usize,
    allowed: impl Fn(char) -> bool,
) -> bool {
    let mut search = text;
    while let Some(index) = search.find(marker) {
        let rest = &search[index + marker.len()..];
        let count = rest.chars().take_while(|c| allowed(*c)).count();
        if count >= minimum {
            return true;
        }
        search = &search[index + marker.len()..];
    }
    false
}

fn contains_aws_key(text: &str) -> bool {
    find_marked_run(text, "AKIA", 16, |c| {
        c.is_ascii_uppercase() || c.is_ascii_digit()
    })
}

fn contains_openai_key(text: &str) -> bool {
    find_marked_run(text, "sk-", 20, |c| c.is_ascii_alphanumeric())
}

fn contains_github_token(text: &str) -> bool {
    find_marked_run(text, "ghp_", 36, |c| c.is_ascii_alphanumeric())
}

fn find_marked_run(
    text: &str,
    marker: &str,
    minimum: usize,
    allowed: impl Fn(char) -> bool,
) -> bool {
    let mut search = text;
    while let Some(index) = search.find(marker) {
        let rest = &search[index + marker.len()..];
        let count = rest.chars().take_while(|c| allowed(*c)).count();
        if count >= minimum {
            return true;
        }
        search = &search[index + marker.len()..];
    }
    false
}

fn contains_private_key_header(text: &str) -> bool {
    let mut search = text;
    while let Some(index) = search.find("-----BEGIN ") {
        let rest = &search[index + "-----BEGIN ".len()..];
        let middle: String = rest
            .chars()
            .take_while(|c| c.is_ascii_uppercase() || *c == ' ')
            .collect();
        if rest[middle.len()..].starts_with("PRIVATE KEY-----") {
            return true;
        }
        search = &search[index + "-----BEGIN ".len()..];
    }
    false
}

fn contains_home_path(text: &str, prefix: &str) -> bool {
    let allowed = |c: char| c.is_ascii_alphanumeric() || "._-".contains(c);
    let mut search = text;
    while let Some(index) = search.find(prefix) {
        let rest = &search[index + prefix.len()..];
        let name: String = rest.chars().take_while(|c| allowed(*c)).collect();
        let after = &rest[name.len()..];
        if !name.is_empty() && after.starts_with('/') {
            return true;
        }
        search = &search[index + prefix.len()..];
    }
    false
}

fn contains_email(text: &str) -> bool {
    for (index, _) in text.match_indices('@') {
        let before: String = text[..index]
            .chars()
            .rev()
            .take_while(|c| c.is_ascii_alphanumeric() || "._%+-".contains(*c))
            .collect();
        if before.is_empty() {
            continue;
        }
        let after = &text[index + 1..];
        let domain: String = after
            .chars()
            .take_while(|c| c.is_ascii_alphanumeric() || ".-".contains(*c))
            .collect();
        if domain.contains('.') {
            let tld = domain.rsplit('.').next().unwrap_or("");
            if tld.len() >= 2 && tld.chars().all(|c| c.is_ascii_alphabetic()) {
                return true;
            }
        }
    }
    false
}

/// When the repository is a real Git worktree, the conversation buffer must not
/// be tracked. In a no-git tarball export (no `.git`) this test is a no-op, so
/// it never fails an offline source-only checkout.
#[test]
fn conversation_buffer_is_not_tracked_when_a_worktree_is_available() {
    let root = repo_root();
    if !root.join(".git").exists() {
        return;
    }
    let output = match std::process::Command::new("git")
        .arg("-C")
        .arg(&root)
        .arg("ls-files")
        .output()
    {
        Ok(output) => output,
        Err(_) => return,
    };
    if !output.status.success() {
        return;
    }
    let tracked = String::from_utf8_lossy(&output.stdout);
    let buffer = concat!("forward_", "to_gpt.md");
    assert!(
        !tracked.lines().any(|line| line == buffer),
        "the conversation buffer must not be tracked by Git"
    );
}
