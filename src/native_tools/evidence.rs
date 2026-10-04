use super::{relative_path, ProjectRoot, ToolError, ToolErrorKind, ToolResult, TOOL_OUTPUT_CAP};
use crate::context::{classify, fulltext, repomap};
use serde::Deserialize;
use serde_json::{json, Value};
use std::collections::BTreeMap;
use std::fs::{self, File};
use std::io::Read;
use std::path::{Component, Path, PathBuf};
use std::time::Instant;

const ITEM_CONTENT_CAP: usize = 8192;

#[derive(Deserialize)]
struct Item {
    path: String,
    line_start: usize,
    line_end: usize,
    revision: String,
}

struct Source {
    revision: Option<String>,
    content: Option<String>,
    stamp: Option<String>,
    problem: Option<&'static str>,
}

fn error(kind: ToolErrorKind, message: impl Into<String>) -> ToolError {
    ToolError::new(kind, message)
}

fn active(cancelled: &dyn Fn() -> bool) -> std::result::Result<(), ToolError> {
    if cancelled() {
        return Err(error(ToolErrorKind::Cancelled, "evidence read cancelled"));
    }
    Ok(())
}

fn allowed(path: &Path) -> std::result::Result<(), ToolError> {
    if path.components().any(|component| match component {
        Component::Normal(name) => name.to_str().is_some_and(|name| {
            name == repomap::OCG_DIR
                || name == ".codegraph"
                || repomap::EXCLUDED_DIRS.contains(&name)
        }),
        _ => false,
    }) || classify::classify(path).sensitive
    {
        return Err(error(
            ToolErrorKind::PermissionDenied,
            "path is excluded from Project context",
        ));
    }
    Ok(())
}

fn resolve(root: &ProjectRoot, raw: &str) -> std::result::Result<Option<PathBuf>, ToolError> {
    let relative = relative_path(raw)?;
    allowed(&relative)?;
    match root.resolve_existing(raw) {
        Ok(path) => {
            allowed(
                path.strip_prefix(root.path()).map_err(|_| {
                    error(ToolErrorKind::PathEscape, "evidence escapes Project root")
                })?,
            )?;
            if !path.is_file() {
                return Err(error(
                    ToolErrorKind::InvalidInput,
                    "evidence path must be a regular file",
                ));
            }
            Ok(Some(path))
        }
        Err(failure) if failure.kind == ToolErrorKind::ExecutionFailure => {
            let mut candidate = relative.as_path();
            loop {
                match fs::symlink_metadata(root.path().join(candidate)) {
                    Ok(metadata) => {
                        let resolved = root
                            .resolve_existing(candidate.to_str().ok_or_else(|| {
                                error(ToolErrorKind::InvalidInput, "invalid evidence path")
                            })?)
                            .map_err(|failure| {
                                if metadata.file_type().is_symlink()
                                    && failure.kind == ToolErrorKind::ExecutionFailure
                                {
                                    error(ToolErrorKind::PathEscape, "unresolved evidence symlink")
                                } else {
                                    failure
                                }
                            })?;
                        allowed(resolved.strip_prefix(root.path()).map_err(|_| {
                            error(ToolErrorKind::PathEscape, "evidence escapes Project root")
                        })?)?;
                        return Ok(None);
                    }
                    Err(error)
                        if matches!(
                            error.kind(),
                            std::io::ErrorKind::NotFound | std::io::ErrorKind::NotADirectory
                        ) => {}
                    Err(_) => return Err(failure),
                }
                candidate = candidate
                    .parent()
                    .filter(|parent| !parent.as_os_str().is_empty())
                    .unwrap_or(Path::new("."));
                if candidate == Path::new(".") {
                    root.resolve_existing(".")?;
                    return Ok(None);
                }
            }
        }
        Err(failure) => Err(failure),
    }
}

fn load(path: &Path, cancelled: &dyn Fn() -> bool) -> std::result::Result<Source, ToolError> {
    active(cancelled)?;
    let mut file = match File::open(path) {
        Ok(file) => file,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            return Ok(Source {
                revision: None,
                content: None,
                stamp: None,
                problem: Some("missing"),
            })
        }
        Err(_) => {
            return Ok(Source {
                revision: None,
                content: None,
                stamp: None,
                problem: Some("unreadable"),
            })
        }
    };
    let metadata = file
        .metadata()
        .map_err(|_| error(ToolErrorKind::ExecutionFailure, "cannot stat evidence file"))?;
    if !metadata.is_file() || metadata.len() > fulltext::MAX_FILE_BYTES {
        return Ok(Source {
            revision: None,
            content: None,
            stamp: None,
            problem: Some("file_size_limit"),
        });
    }
    let stamp = fulltext::metadata_stamp(&metadata);
    let mut bytes = Vec::new();
    let mut buffer = [0u8; 64 * 1024];
    loop {
        active(cancelled)?;
        let available = (fulltext::MAX_FILE_BYTES as usize + 1 - bytes.len()).min(buffer.len());
        let count = match file.read(&mut buffer[..available]) {
            Ok(count) => count,
            Err(error) if error.kind() == std::io::ErrorKind::Interrupted => continue,
            Err(_) => {
                return Ok(Source {
                    revision: None,
                    content: None,
                    stamp: None,
                    problem: Some("unreadable"),
                })
            }
        };
        if count == 0 {
            break;
        }
        bytes.extend_from_slice(&buffer[..count]);
        if bytes.len() as u64 > fulltext::MAX_FILE_BYTES {
            return Ok(Source {
                revision: None,
                content: None,
                stamp: None,
                problem: Some("file_size_limit"),
            });
        }
    }
    active(cancelled)?;
    let revision = format!("sha256:{}", crate::hash::sha256_hex(&bytes));
    let content = if bytes.contains(&0) {
        None
    } else {
        String::from_utf8(bytes).ok()
    };
    Ok(Source {
        revision: Some(revision),
        content,
        stamp: Some(stamp),
        problem: None,
    })
}

pub(super) fn read(
    root: &ProjectRoot,
    arguments: &Value,
    cancelled: &dyn Fn() -> bool,
) -> ToolResult {
    match read_batch(root, arguments, cancelled) {
        Ok(result) => result,
        Err(error) => ToolResult::failure(error),
    }
}

fn read_batch(
    root: &ProjectRoot,
    arguments: &Value,
    cancelled: &dyn Fn() -> bool,
) -> std::result::Result<ToolResult, ToolError> {
    let started = Instant::now();
    let requested = arguments
        .get("items")
        .and_then(Value::as_array)
        .ok_or_else(|| error(ToolErrorKind::InvalidInput, "invalid evidence items"))?;
    if requested.is_empty() || requested.len() > 16 {
        return Err(error(
            ToolErrorKind::InvalidInput,
            "context.read requires 1..16 items",
        ));
    }
    let items: Vec<Item> = serde_json::from_value(Value::Array(requested.clone()))
        .map_err(|_| error(ToolErrorKind::InvalidInput, "invalid evidence items"))?;
    let mut paths = Vec::with_capacity(items.len());
    for item in &items {
        active(cancelled)?;
        if item.line_start == 0
            || item.line_end < item.line_start
            || item.line_end - item.line_start >= 400
            || item.path.len() > 4096
            || item.revision.len() != 71
            || !item.revision.starts_with("sha256:")
            || !item
                .revision
                .bytes()
                .skip(7)
                .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
        {
            return Err(error(
                ToolErrorKind::InvalidInput,
                "invalid path, inclusive line range (maximum 400 lines), or sha256 revision",
            ));
        }
        paths.push(resolve(root, &item.path)?);
    }
    let mut sources = BTreeMap::new();
    let mut files_hashed = 0;
    for path in paths.iter().flatten() {
        active(cancelled)?;
        if !sources.contains_key(path) {
            let source = load(path, cancelled)?;
            files_hashed += usize::from(source.revision.is_some());
            sources.insert(path.clone(), source);
        }
    }
    // Validate after all reads, including path resolution: replacements during a
    // batch must not turn content from an old handle into current evidence.
    for (item, initial) in items.iter().zip(&paths) {
        active(cancelled)?;
        let current = resolve(root, &item.path)?;
        if let Some(path) = initial {
            let source = sources
                .get_mut(path)
                .ok_or_else(|| error(ToolErrorKind::ExecutionFailure, "missing batch source"))?;
            if current.is_none() {
                source.problem = Some("missing");
                source.content = None;
            } else if current.as_ref() != Some(path)
                || source.stamp.as_ref().is_some_and(|stamp| {
                    fs::metadata(path)
                        .map(|metadata| fulltext::metadata_stamp(&metadata) != *stamp)
                        .unwrap_or(true)
                })
            {
                source.problem = Some("changed_during_read");
                source.revision = None;
                source.content = None;
            }
        }
    }
    let mut outcomes = Vec::new();
    let mut ranges = Vec::new();
    let mut stale = false;
    for (item, path) in items.iter().zip(&paths) {
        active(cancelled)?;
        let mut outcome = json!({"path":item.path,"line_start":item.line_start,"line_end":item.line_end,"requested_revision":item.revision});
        let source = path.as_ref().and_then(|path| sources.get(path));
        let mut range = None;
        match source {
            None => outcome["status"] = json!("missing"),
            Some(source) if source.problem == Some("missing") => {
                outcome["status"] = json!("missing")
            }
            Some(source)
                if source.problem == Some("changed_during_read")
                    || source
                        .revision
                        .as_ref()
                        .is_some_and(|revision| revision != &item.revision) =>
            {
                stale = true;
                outcome["status"] = json!("stale");
                outcome["current_revision"] = json!(source.revision);
                outcome["message"] = json!("Evidence changed; run context.search again.");
            }
            Some(source) => match source.content.as_deref() {
                None => {
                    outcome["status"] = json!("unavailable");
                    outcome["reason"] = json!(source.problem.unwrap_or("binary"));
                }
                Some(content) => {
                    let mut offset = 0;
                    let mut start = None;
                    let mut end = 0;
                    let mut complete = false;
                    for (index, line) in content.split_inclusive('\n').enumerate() {
                        let number = index + 1;
                        if number == item.line_start {
                            start = Some(offset);
                        }
                        offset += line.len();
                        if number >= item.line_start && number <= item.line_end {
                            end = offset;
                        }
                        if number == item.line_end {
                            complete = true;
                            break;
                        }
                    }
                    if let Some(start) = start.filter(|_| complete) {
                        outcome["status"] = json!("ok");
                        outcome["revision"] = json!(item.revision);
                        outcome["content"] = json!("");
                        outcome["truncated"] = json!(false);
                        range = Some((start, end));
                    } else {
                        outcome["status"] = json!("out_of_range");
                    }
                }
            },
        }
        outcomes.push(outcome);
        ranges.push(range);
    }
    let mut result = ToolResult::success(json!({"items":outcomes,"stale":stale,"truncated":false}));
    result.metadata = json!({"requested_items":items.len(),"unique_files":sources.len(),"files_read":sources.len(),"files_hashed":files_hashed,"returned_bytes":0,"elapsed_ms":0});
    let base_bytes = result.to_value().to_string().len();
    if base_bytes + 512 > TOOL_OUTPUT_CAP {
        return Err(error(
            ToolErrorKind::InvalidInput,
            "evidence item metadata exceeds output budget",
        ));
    }
    let mut budget = TOOL_OUTPUT_CAP - base_bytes - 512;
    let mut returned_bytes = 0;
    for (index, range) in ranges.iter().enumerate() {
        active(cancelled)?;
        if let (Some((start, end)), Some(source)) = (
            range,
            paths
                .get(index)
                .and_then(Option::as_ref)
                .and_then(|path| sources.get(path)),
        ) {
            let content = source
                .content
                .as_deref()
                .and_then(|text| text.get(*start..*end))
                .ok_or_else(|| error(ToolErrorKind::ExecutionFailure, "invalid evidence range"))?;
            let mut bytes = 0;
            for character in content.chars() {
                let encoded = match character {
                    '"' | '\\' | '\n' | '\r' | '\t' | '\u{8}' | '\u{c}' => 2,
                    character if character <= '\u{1f}' => 6,
                    character => character.len_utf8(),
                };
                if bytes + character.len_utf8() > ITEM_CONTENT_CAP || encoded > budget {
                    break;
                }
                budget -= encoded;
                bytes += character.len_utf8();
            }
            let clipped = bytes < content.len();
            result.output["items"][index]["content"] = json!(content.get(..bytes).ok_or_else(
                || error(ToolErrorKind::ExecutionFailure, "invalid evidence boundary")
            )?);
            result.output["items"][index]["truncated"] = json!(clipped);
            returned_bytes += bytes;
        }
    }
    for (index, (item, initial)) in items.iter().zip(&paths).enumerate() {
        active(cancelled)?;
        if result.output["items"][index]["status"] != "ok" {
            continue;
        }
        let current = resolve(root, &item.path)?;
        let unchanged = current.as_ref() == initial.as_ref()
            && initial
                .as_ref()
                .and_then(|path| sources.get(path))
                .is_some_and(|source| {
                    current
                        .as_ref()
                        .and_then(|path| fs::metadata(path).ok())
                        .is_some_and(|metadata| {
                            source.stamp.as_ref() == Some(&fulltext::metadata_stamp(&metadata))
                        })
                });
        if !unchanged {
            let outcome = result.output["items"][index]
                .as_object_mut()
                .ok_or_else(|| {
                    error(ToolErrorKind::ExecutionFailure, "invalid evidence outcome")
                })?;
            returned_bytes -= outcome
                .get("content")
                .and_then(Value::as_str)
                .map(str::len)
                .unwrap_or(0);
            outcome.remove("content");
            outcome.remove("revision");
            outcome.remove("truncated");
            if current.is_none() {
                outcome.insert("status".into(), json!("missing"));
            } else {
                stale = true;
                outcome.insert("status".into(), json!("stale"));
                outcome.insert("current_revision".into(), Value::Null);
                outcome.insert(
                    "message".into(),
                    json!("Evidence changed; run context.search again."),
                );
            }
        }
    }
    let truncated = result.output["items"]
        .as_array()
        .is_some_and(|items| items.iter().any(|item| item["truncated"] == true));
    result.truncated = truncated;
    result.output["stale"] = json!(stale);
    result.output["truncated"] = json!(truncated);
    result.metadata["returned_bytes"] = json!(returned_bytes);
    result.metadata["elapsed_ms"] = json!(started.elapsed().as_millis());
    Ok(result)
}
