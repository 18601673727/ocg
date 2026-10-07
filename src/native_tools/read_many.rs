use super::{
    bounded_text, read_file_bytes, relative_path, ProjectRoot, ToolError, ToolErrorKind, ToolResult,
};
use serde_json::{json, Value};
use std::collections::BTreeMap;
use std::fs::{self, File};
use std::path::Path;

const RESULT_CAP: usize = 32 * 1024;
const CONTENT_CAP: usize = 8192;
const LINE_CAP: u64 = 400;

struct Observation {
    start: u64,
    end: u64,
    requested_end: Option<u64>,
    bytes: Vec<u8>,
    seen_start: bool,
    seen_end: bool,
    truncated: bool,
}

fn active(cancelled: &dyn Fn() -> bool) -> std::result::Result<(), ToolError> {
    if cancelled() {
        Err(ToolError::new(
            ToolErrorKind::Cancelled,
            "file read cancelled",
        ))
    } else {
        Ok(())
    }
}

fn observation(item: &Value) -> Option<Observation> {
    let (start, requested_end) = match (
        item.get("line_start").filter(|value| !value.is_null()),
        item.get("line_end").filter(|value| !value.is_null()),
    ) {
        (None, None) => (1, None),
        (Some(start), Some(end)) => (start.as_u64()?, Some(end.as_u64()?)),
        _ => return None,
    };
    if start == 0 || requested_end.is_some_and(|end| end < start) {
        return None;
    }
    let end = requested_end
        .unwrap_or(u64::MAX)
        .min(start.saturating_add(LINE_CAP - 1));
    Some(Observation {
        start,
        end,
        requested_end,
        bytes: Vec::new(),
        seen_start: false,
        seen_end: false,
        truncated: requested_end.is_some_and(|requested| requested > end),
    })
}

fn observe(
    path: &Path,
    indices: &[usize],
    observations: &mut [Option<Observation>],
    cancelled: &dyn Fn() -> bool,
) -> std::result::Result<(), ToolError> {
    active(cancelled)?;
    let mut file = File::open(path).map_err(|error| {
        let mut failure = ToolError::new(ToolErrorKind::ExecutionFailure, "cannot open file");
        failure.metadata = json!({"io_kind":format!("{:?}", error.kind())});
        failure
    })?;
    if !file
        .metadata()
        .map_err(|_| ToolError::new(ToolErrorKind::ExecutionFailure, "cannot stat file"))?
        .is_file()
    {
        return Err(ToolError::new(
            ToolErrorKind::InvalidInput,
            "path must be a regular file",
        ));
    }
    let last = indices
        .iter()
        .filter_map(|index| observations.get(*index)?.as_ref())
        .map(|range| range.end)
        .max()
        .unwrap_or(0);
    let mut number = 1u64;
    loop {
        active(cancelled)?;
        let chunk = read_file_bytes(&mut file, 64 * 1024)?;
        if chunk.is_empty() {
            break;
        }
        for segment in chunk.split_inclusive(|byte| *byte == b'\n') {
            for index in indices {
                if let Some(range) = observations.get_mut(*index).and_then(Option::as_mut) {
                    if number >= range.start && number <= range.end {
                        range.seen_start = true;
                        range.seen_end |= number == range.end;
                        let available = CONTENT_CAP.saturating_sub(range.bytes.len());
                        range
                            .bytes
                            .extend_from_slice(&segment[..segment.len().min(available)]);
                        range.truncated |= segment.len() > available;
                    } else if number > range.end && range.requested_end.is_none() {
                        range.truncated = true;
                    }
                }
            }
            if number > last {
                return Ok(());
            }
            if segment.last() == Some(&b'\n') {
                number = number.saturating_add(1);
            }
        }
    }
    Ok(())
}

fn failure(outcome: &mut Value, status: &str, error: ToolError) {
    outcome["status"] = json!(status);
    outcome["error"] = error.json();
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
    let items = arguments
        .get("items")
        .and_then(Value::as_array)
        .filter(|items| !items.is_empty() && items.len() <= 16)
        .ok_or_else(|| {
            ToolError::new(
                ToolErrorKind::InvalidInput,
                "filesystem.read_many requires 1..16 items",
            )
        })?;
    let mut outcomes = Vec::with_capacity(items.len());
    let mut observations = Vec::with_capacity(items.len());
    let mut files = BTreeMap::new();
    for (index, item) in items.iter().enumerate() {
        active(cancelled)?;
        let mut outcome = json!({"path":item["path"],"line_start":item.get("line_start"),"line_end":item.get("line_end"),"status":"ok","content":"","truncated":false,"aggregate_limited":false});
        let range = observation(item);
        if range.is_none() {
            failure(
                &mut outcome,
                "invalid_range",
                ToolError::new(
                    ToolErrorKind::InvalidInput,
                    "line_start/line_end must be paired, positive, inclusive and ordered",
                ),
            );
        } else if let Some(raw) = item["path"].as_str() {
            match root.resolve_existing(raw) {
                Ok(path) if path.is_file() => {
                    files.entry(path).or_insert_with(Vec::new).push(index)
                }
                Ok(_) => failure(
                    &mut outcome,
                    "error",
                    ToolError::new(ToolErrorKind::InvalidInput, "path must be a regular file"),
                ),
                Err(error) => {
                    let missing = error.kind == ToolErrorKind::ExecutionFailure
                        && relative_path(raw).ok().is_some_and(|relative| {
                            fs::metadata(root.path().join(relative)).is_err_and(|error| {
                                matches!(
                                    error.kind(),
                                    std::io::ErrorKind::NotFound
                                        | std::io::ErrorKind::NotADirectory
                                )
                            })
                        });
                    failure(
                        &mut outcome,
                        if missing { "missing" } else { "error" },
                        error,
                    );
                }
            }
        } else {
            failure(
                &mut outcome,
                "error",
                ToolError::new(ToolErrorKind::InvalidInput, "path is required"),
            );
        }
        outcomes.push(outcome);
        observations.push(range);
    }
    let mut result = ToolResult::success(json!({"items":outcomes,"partial":false}));
    result.metadata = json!({"unique_files":files.len(),"output_cap":RESULT_CAP});
    // Requested identities cannot be discarded to make room for content.
    if result.tool_message_content().len() > RESULT_CAP {
        return Err(ToolError::new(
            ToolErrorKind::OutputLimit,
            "requested item metadata exceeds the 32768 byte result cap",
        ));
    }
    for (path, indices) in &files {
        if let Err(error) = observe(path, indices, &mut observations, cancelled) {
            if error.kind == ToolErrorKind::Cancelled {
                return Err(error);
            }
            let status = if error.metadata["io_kind"] == "NotFound" {
                "missing"
            } else {
                "error"
            };
            for index in indices {
                failure(&mut result.output["items"][*index], status, error.clone());
            }
        }
    }
    for (index, range) in observations.into_iter().enumerate() {
        active(cancelled)?;
        let outcome = &mut result.output["items"][index];
        if outcome["status"] != "ok" {
            continue;
        }
        if let Some(range) = range {
            if range.requested_end.is_some() && (!range.seen_start || !range.seen_end) {
                failure(
                    outcome,
                    "invalid_range",
                    ToolError::new(
                        ToolErrorKind::InvalidInput,
                        "requested range extends beyond end of file",
                    ),
                );
                continue;
            }
            let (mut content, _) = bounded_text(&range.bytes, CONTENT_CAP);
            let mut end = content.len().min(CONTENT_CAP);
            while !content.is_char_boundary(end) {
                end -= 1;
            }
            let truncated = range.truncated || end < content.len();
            content.truncate(end);
            outcome["content"] = json!(content);
            outcome["truncated"] = json!(truncated);
            if truncated {
                outcome["status"] = json!("truncated");
            }
        }
    }
    result.truncated = result.output["items"]
        .as_array()
        .is_some_and(|items| items.iter().any(|item| item["truncated"] == true));
    result.output["partial"] = json!(result.output["items"]
        .as_array()
        .is_some_and(|items| items.iter().any(|item| item["status"] != "ok")));
    fit_result(result, cancelled)
}

fn fit_result(
    mut result: ToolResult,
    cancelled: &dyn Fn() -> bool,
) -> std::result::Result<ToolResult, ToolError> {
    let count = result.output["items"].as_array().map_or(0, Vec::len);
    for index in (0..count).rev() {
        active(cancelled)?;
        if result.tool_message_content().len() <= RESULT_CAP {
            return Ok(result);
        }
        let content = result.output["items"][index]["content"]
            .as_str()
            .unwrap_or("")
            .to_owned();
        if content.is_empty() {
            continue;
        }
        result.truncated = true;
        result.output["partial"] = json!(true);
        result.output["items"][index]["truncated"] = json!(true);
        result.output["items"][index]["aggregate_limited"] = json!(true);
        result.output["items"][index]["status"] = json!("truncated");
        let mut low = 0;
        let mut high = content.len();
        while low < high {
            let middle = low + (high - low).div_ceil(2);
            let mut end = middle;
            while !content.is_char_boundary(end) {
                end -= 1;
            }
            result.output["items"][index]["content"] = json!(&content[..end]);
            if result.tool_message_content().len() <= RESULT_CAP {
                low = middle;
            } else {
                high = middle - 1;
            }
        }
        while !content.is_char_boundary(low) {
            low -= 1;
        }
        result.output["items"][index]["content"] = json!(&content[..low]);
    }
    if result.tool_message_content().len() <= RESULT_CAP {
        Ok(result)
    } else {
        Err(ToolError::new(
            ToolErrorKind::OutputLimit,
            "requested item metadata exceeds the 32768 byte result cap",
        ))
    }
}
