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
        OperationKind::Append => {
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
    if file.is_empty() || target.is_some_and(str::is_empty) {
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
    Ok((crate::runtime::hash::sha256_hex(&bytes), bytes))
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
    if (kind == OperationKind::Append) != call.request.target.is_none()
        || call.request.target.as_deref().is_some_and(str::is_empty)
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
    let lock_name = crate::runtime::hash::sha256_hex(path.to_string_lossy().as_bytes());
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
        if stale && kind == OperationKind::Append {
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
            new_revision: crate::runtime::hash::sha256_hex(next.as_bytes()),
        });
    }
}

fn resolve(text: &str, request: &EditRequest) -> Result<(String, bool), Conflict> {
    let Some(target) = request.target.as_deref() else {
        return if request.kind == OperationKind::Append {
            Ok((format!("{text}{}", request.content), false))
        } else {
            Err(Conflict::InvalidRequest)
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
        OperationKind::Append => return Err(Conflict::InvalidRequest),
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
