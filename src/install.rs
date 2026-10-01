//! Safe install and filesystem utilities.
//!
//! Generic helpers for atomic file writes and `.ocg/` gitignore management.

use crate::error::{OcgError, Result};
use serde_json::Value;
use std::fs;
use std::io::Write;
use std::path::Path;

const GITIGNORE_ENTRY: &str = ".ocg/";

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
