//! Standalone tool-call construction boundary and transactional text-file editor.
//! Callers supply a project root; no orchestration or provider state is required.
use fs2::FileExt;
use serde_json::{Map, Value};
use std::fs::{self, OpenOptions};
use std::io::Write;
use std::path::{Component, Path, PathBuf};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OperationKind {
    Replace,
    InsertBefore,
    InsertAfter,
    Append,
    AppendLine,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EditRequest {
    pub file: PathBuf,
    /// SHA-256 of the bytes observed when constructing this request, if known.
    pub expected_revision: Option<String>,
    pub kind: OperationKind,
    /// Exact target for Replace, or exact anchor for InsertBefore/InsertAfter.
    pub target: Option<String>,
    pub content: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FailureClass {
    Construction,
    Execution,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Conflict {
    InvalidRequest,
    MissingTarget,
    AmbiguousTarget,
    StaleRevision,
    ConcurrentModification,
    UnsupportedOperation,
    UnsafeRepair,
    IOFailure,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EditFailure {
    pub class: FailureClass,
    pub conflict: Conflict,
    pub kind: Option<OperationKind>,
    pub retries: u8,
    pub revision: Option<String>,
}

impl EditFailure {
    fn new(class: FailureClass, conflict: Conflict, kind: Option<OperationKind>) -> Self {
        Self {
            class,
            conflict,
            kind,
            retries: 0,
            revision: None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CanonicalCall {
    pub request: EditRequest,
    pub mechanically_repaired: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EditOutcome {
    pub kind: OperationKind,
    pub direct: bool,
    pub mechanically_repaired: bool,
    pub rebased: bool,
    pub retries: u8,
    pub previous_revision: String,
    pub new_revision: String,
}

/// Accept only a small fixed schema. `old_string` is the one documented legacy
/// alias: it is never inferred from the file or from semantic context.
pub fn construct_call(value: &Value) -> Result<CanonicalCall, EditFailure> {
    let bad = |conflict| EditFailure::new(FailureClass::Construction, conflict, None);
    let obj = value
        .as_object()
        .ok_or_else(|| bad(Conflict::InvalidRequest))?;
    let kind = match obj.get("operation").and_then(Value::as_str) {
        Some("replace") => OperationKind::Replace,
        Some("insertBefore") => OperationKind::InsertBefore,
        Some("insertAfter") => OperationKind::InsertAfter,
        Some("append") => OperationKind::Append,
        Some("appendLine") => OperationKind::AppendLine,
        Some(_) => return Err(bad(Conflict::UnsupportedOperation)),
        None => return Err(bad(Conflict::InvalidRequest)),
    };
    let invalid = || {
        EditFailure::new(
            FailureClass::Construction,
            Conflict::InvalidRequest,
            Some(kind),
        )
    };
    let allowed = [
        "operation",
        "file",
        "expectedRevision",
        "oldString",
        "old_string",
        "anchor",
        "newString",
        "content",
    ];
    if obj.keys().any(|k| !allowed.contains(&k.as_str())) {
        return Err(invalid());
    }
    let file = string(obj, "file").ok_or_else(invalid)?;
    let expected_revision = match obj.get("expectedRevision") {
        None => None,
        Some(Value::String(s)) if s.len() == 64 && s.bytes().all(|b| b.is_ascii_hexdigit()) => {
            Some(s.to_ascii_lowercase())
        }
        _ => return Err(invalid()),
    };
    let (target, content, repaired) = match kind {
        OperationKind::Replace => {
            if obj.contains_key("anchor")
                || obj.contains_key("content")
                || (obj.contains_key("oldString") && obj.contains_key("old_string"))
            {
                return Err(invalid());
            }
            let alias = obj.contains_key("old_string");
            (
                Some(
                    string(obj, if alias { "old_string" } else { "oldString" })
                        .ok_or_else(invalid)?,
                ),
                string(obj, "newString").ok_or_else(invalid)?,
                alias,
            )
        }
        OperationKind::InsertBefore | OperationKind::InsertAfter => {
            if obj.contains_key("oldString")
                || obj.contains_key("old_string")
                || obj.contains_key("newString")
            {
                return Err(invalid());
            }
            (
                Some(string(obj, "anchor").ok_or_else(invalid)?),
                string(obj, "content").ok_or_else(invalid)?,
                false,
            )
        }
        OperationKind::Append | OperationKind::AppendLine => {
            if obj.contains_key("anchor")
                || obj.contains_key("oldString")
                || obj.contains_key("old_string")
                || obj.contains_key("newString")
            {
                return Err(invalid());
            }
            (None, string(obj, "content").ok_or_else(invalid)?, false)
        }
    };
    if file.is_empty()
        || target.is_some_and(str::is_empty)
        || (kind == OperationKind::AppendLine && content.contains(['\r', '\n']))
    {
        return Err(invalid());
    }
    Ok(CanonicalCall {
        request: EditRequest {
            file: PathBuf::from(file),
            expected_revision,
            kind,
            target: target.map(str::to_owned),
            content: content.to_owned(),
        },
        mechanically_repaired: repaired,
    })
}

fn string<'a>(obj: &'a Map<String, Value>, key: &str) -> Option<&'a str> {
    obj.get(key).and_then(Value::as_str)
}

/// Return the SHA-256 revision of an existing file. Callers can attach it to
/// a subsequent tool call; a missing revision never authorizes a blind rewrite.
pub fn read_revision(path: &Path) -> std::io::Result<(String, Vec<u8>)> {
    let bytes = fs::read(path)?;
    Ok((crate::hash::sha256_hex(&bytes), bytes))
}

pub fn apply_call(root: &Path, call: CanonicalCall) -> Result<EditOutcome, EditFailure> {
    apply_inner(root, call, |_| {})
}

// A private deterministic race seam; production always uses the no-op hook.
fn apply_inner(
    root: &Path,
    call: CanonicalCall,
    mut before_commit: impl FnMut(u8),
) -> Result<EditOutcome, EditFailure> {
    const MAX_RETRIES: u8 = 2;
    let kind = call.request.kind;
    let fail = |conflict| EditFailure::new(FailureClass::Execution, conflict, Some(kind));
    let appends = matches!(kind, OperationKind::Append | OperationKind::AppendLine);
    if appends != call.request.target.is_none()
        || call.request.target.as_deref().is_some_and(str::is_empty)
        || (kind == OperationKind::AppendLine && call.request.content.contains(['\r', '\n']))
        || call
            .request
            .expected_revision
            .as_ref()
            .is_some_and(|hash| hash.len() != 64 || !hash.bytes().all(|b| b.is_ascii_hexdigit()))
    {
        return Err(fail(Conflict::InvalidRequest));
    }
    let relative = &call.request.file;
    if relative.is_absolute()
        || !relative
            .components()
            .all(|c| matches!(c, Component::Normal(_)))
    {
        return Err(fail(Conflict::InvalidRequest));
    }
    let root = root.canonicalize().map_err(|_| fail(Conflict::IOFailure))?;
    let path = root.join(relative);
    if path.canonicalize().map_err(|_| fail(Conflict::IOFailure))? != path {
        return Err(fail(Conflict::UnsafeRepair));
    }
    // Per-file cooperative lock, placed under the project's ignored state dir.
    let lock_dir = root.join(".ocg/edit-locks");
    fs::create_dir_all(&lock_dir).map_err(|_| fail(Conflict::IOFailure))?;
    let lock_name = crate::hash::sha256_hex(path.to_string_lossy().as_bytes());
    let lock = OpenOptions::new()
        .create(true)
        .truncate(false)
        .read(true)
        .write(true)
        .open(lock_dir.join(lock_name))
        .map_err(|_| fail(Conflict::IOFailure))?;
    lock.lock_exclusive()
        .map_err(|_| fail(Conflict::IOFailure))?;
    let mut retries = 0;
    loop {
        let (revision, bytes) = read_revision(&path).map_err(|_| fail(Conflict::IOFailure))?;
        let text = std::str::from_utf8(&bytes).map_err(|_| fail(Conflict::UnsupportedOperation))?;
        let stale = call
            .request
            .expected_revision
            .as_ref()
            .is_some_and(|expected| expected != &revision);
        if stale && appends {
            return Err(EditFailure {
                revision: Some(revision),
                retries,
                ..fail(Conflict::StaleRevision)
            });
        }
        let (next, recovered) = resolve(text, &call.request).map_err(|conflict| EditFailure {
            revision: Some(revision.clone()),
            retries,
            ..fail(if stale && conflict == Conflict::MissingTarget {
                Conflict::StaleRevision
            } else {
                conflict
            })
        })?;
        if stale && recovered {
            // no additional semantic recovery on a stale snapshot
            return Err(EditFailure {
                revision: Some(revision),
                retries,
                ..fail(Conflict::StaleRevision)
            });
        }
        let parent = path.parent().expect("file has parent");
        let mut temp = tempfile::Builder::new()
            .prefix(".ocg-edit-")
            .tempfile_in(parent)
            .map_err(|_| fail(Conflict::IOFailure))?;
        // Preserve permissions on replacement (notably executable bits).
        let permissions = fs::metadata(&path)
            .map_err(|_| fail(Conflict::IOFailure))?
            .permissions();
        temp.as_file()
            .set_permissions(permissions)
            .map_err(|_| fail(Conflict::IOFailure))?;
        temp.write_all(next.as_bytes())
            .and_then(|_| temp.as_file().sync_all())
            .map_err(|_| fail(Conflict::IOFailure))?;
        before_commit(retries);
        let (latest, _) = read_revision(&path).map_err(|_| fail(Conflict::IOFailure))?;
        if latest != revision {
            if retries == MAX_RETRIES {
                return Err(EditFailure {
                    revision: Some(latest),
                    retries,
                    ..fail(Conflict::ConcurrentModification)
                });
            }
            retries += 1;
            continue;
        }
        temp.persist(&path).map_err(|_| fail(Conflict::IOFailure))?;
        // Durability of the directory entry on Unix; rename is atomic on the
        // same filesystem. Other platforms retain the rename guarantee only.
        #[cfg(unix)]
        fs::File::open(parent)
            .and_then(|dir| dir.sync_all())
            .map_err(|_| fail(Conflict::IOFailure))?;
        return Ok(EditOutcome {
            kind,
            direct: !stale && !recovered && retries == 0 && !call.mechanically_repaired,
            mechanically_repaired: call.mechanically_repaired || recovered,
            rebased: stale || retries > 0,
            retries,
            previous_revision: revision,
            new_revision: crate::hash::sha256_hex(next.as_bytes()),
        });
    }
}

fn resolve(text: &str, request: &EditRequest) -> Result<(String, bool), Conflict> {
    let Some(target) = request.target.as_deref() else {
        return match request.kind {
            OperationKind::Append => Ok((format!("{text}{}", request.content), false)),
            OperationKind::AppendLine => {
                let mut next = text.to_owned();
                if !text.is_empty() && !text.ends_with('\n') {
                    next.push('\n');
                }
                next.push_str(&request.content);
                next.push('\n');
                Ok((next, false))
            }
            _ => Err(Conflict::InvalidRequest),
        };
    };
    if target.is_empty() {
        return Err(Conflict::InvalidRequest);
    }
    let (needle, recovered) = if text.contains(target) {
        (target.to_owned(), false)
    } else if target.contains('\n') && text.contains("\r\n") && !target.contains("\r\n") {
        (target.replace('\n', "\r\n"), true)
    } else if target.contains("\r\n") {
        (target.replace("\r\n", "\n"), true)
    } else {
        return Err(Conflict::MissingTarget);
    };
    let mut positions = text.match_indices(&needle);
    let (at, _) = positions.next().ok_or(Conflict::MissingTarget)?;
    if positions.next().is_some() {
        return Err(Conflict::AmbiguousTarget);
    }
    let position = match request.kind {
        OperationKind::Replace | OperationKind::InsertBefore => at,
        OperationKind::InsertAfter => at + needle.len(),
        OperationKind::Append | OperationKind::AppendLine => return Err(Conflict::InvalidRequest),
    };
    let end = if request.kind == OperationKind::Replace {
        at + needle.len()
    } else {
        position
    };
    let mut next = String::with_capacity(text.len() + request.content.len());
    next.push_str(&text[..position]);
    next.push_str(&request.content);
    next.push_str(&text[end..]);
    Ok((next, recovered))
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn append_line(initial: &[u8]) -> Vec<u8> {
        let root = tempfile::tempdir().unwrap();
        let path = root.path().join("README.md");
        fs::write(&path, initial).unwrap();
        let call = construct_call(&json!({
            "operation": "appendLine", "file": "README.md", "content": "Acceptance write test."
        }))
        .unwrap();
        let outcome = apply_call(root.path(), call).unwrap();
        let bytes = fs::read(&path).unwrap();
        assert_eq!(outcome.kind, OperationKind::AppendLine);
        assert_eq!(outcome.previous_revision, crate::hash::sha256_hex(initial));
        assert_eq!(outcome.new_revision, crate::hash::sha256_hex(&bytes));
        assert!(outcome.direct);
        assert!(!outcome.rebased);
        assert!(!outcome.mechanically_repaired);
        assert_eq!(outcome.retries, 0);
        bytes
    }

    #[test]
    fn append_line_to_terminated_file() {
        assert_eq!(
            append_line(b"# OCG Human Acceptance\n"),
            b"# OCG Human Acceptance\nAcceptance write test.\n"
        );
    }

    #[test]
    fn append_line_to_unterminated_file() {
        assert_eq!(
            append_line(b"# OCG Human Acceptance"),
            b"# OCG Human Acceptance\nAcceptance write test.\n"
        );
    }

    #[test]
    fn append_line_to_empty_file() {
        assert_eq!(append_line(b""), b"Acceptance write test.\n");
    }

    #[test]
    fn append_line_preserves_existing_crlf_bytes() {
        assert_eq!(
            append_line(b"# OCG Human Acceptance\r\n"),
            b"# OCG Human Acceptance\r\nAcceptance write test.\n"
        );
    }

    #[test]
    fn append_line_rejects_cr_and_lf_content() {
        for content in [
            "\n",
            "\r",
            "first\nsecond",
            "first\rsecond",
            "first\r\nsecond",
        ] {
            let error = construct_call(&json!({
                "operation": "appendLine", "file": "README.md", "content": content
            }))
            .unwrap_err();
            assert_eq!(error.class, FailureClass::Construction);
            assert_eq!(error.conflict, Conflict::InvalidRequest);
            assert_eq!(error.kind, Some(OperationKind::AppendLine));
        }
    }

    #[test]
    fn append_line_stale_revision_leaves_file_unchanged() {
        let root = tempfile::tempdir().unwrap();
        let path = root.path().join("README.md");
        let initial = b"# OCG Human Acceptance\n";
        fs::write(&path, initial).unwrap();
        let call = construct_call(&json!({
            "operation": "appendLine", "file": "README.md", "content": "Acceptance write test.",
            "expectedRevision": "0".repeat(64)
        }))
        .unwrap();
        let error = apply_call(root.path(), call).unwrap_err();
        assert_eq!(error.class, FailureClass::Execution);
        assert_eq!(error.conflict, Conflict::StaleRevision);
        assert_eq!(error.revision, Some(crate::hash::sha256_hex(initial)));
        assert_eq!(fs::read(path).unwrap(), initial);
    }

    #[test]
    fn raw_append_preserves_exact_content_without_adding_newlines() {
        let root = tempfile::tempdir().unwrap();
        let path = root.path().join("README.md");
        fs::write(&path, b"original").unwrap();
        for (content, expected) in [
            ("suffix", "originalsuffix"),
            ("\r\nnext\n", "originalsuffix\r\nnext\n"),
            ("", "originalsuffix\r\nnext\n"),
        ] {
            let call = construct_call(&json!({
                "operation": "append", "file": "README.md", "content": content
            }))
            .unwrap();
            assert_eq!(
                apply_call(root.path(), call).unwrap().kind,
                OperationKind::Append
            );
            assert_eq!(fs::read(&path).unwrap(), expected.as_bytes());
        }
    }

    #[test]
    fn append_line_execution_rejects_invalid_direct_request() {
        let root = tempfile::tempdir().unwrap();
        let path = root.path().join("README.md");
        fs::write(&path, b"original").unwrap();
        let mut call = construct_call(&json!({
            "operation": "appendLine", "file": "README.md", "content": "line"
        }))
        .unwrap();
        call.request.content.push('\n');
        let error = apply_call(root.path(), call).unwrap_err();
        assert_eq!(error.class, FailureClass::Execution);
        assert_eq!(error.conflict, Conflict::InvalidRequest);
        assert_eq!(fs::read(path).unwrap(), b"original");
    }

    #[test]
    fn append_line_concurrency_retry_does_not_duplicate_line() {
        let root = tempfile::tempdir().unwrap();
        let path = root.path().join("README.md");
        fs::write(&path, b"original\n").unwrap();
        let call = construct_call(&json!({
            "operation": "appendLine", "file": "README.md", "content": "line"
        }))
        .unwrap();
        let outcome = apply_inner(root.path(), call, |retry| {
            if retry == 0 {
                fs::write(&path, b"original\nconcurrent\n").unwrap();
            }
        })
        .unwrap();
        assert_eq!(outcome.retries, 1);
        assert!(outcome.rebased);
        assert!(!outcome.mechanically_repaired);
        assert_eq!(fs::read(path).unwrap(), b"original\nconcurrent\nline\n");
    }
}
