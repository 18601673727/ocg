//! Local STDIO Model Context Protocol adapter over [`ControlService`].
//!
//! This module owns protocol framing and tool schemas only. It has no state
//! store and never reads a Mission, approval, resource or budget projection.
//! Every tool call enters the existing transport-neutral application boundary.

use crate::error::{OcgError, Result};
use crate::orchestration::budget::{normalize_currency, Money};
use crate::orchestration::checkpoint::is_safe_id;
use crate::orchestration::control::{ControlError, ControlService, ReplaySlice};
use crate::orchestration::policy::{ApprovalStatus, MAX_REASON_BYTES};
use serde_json::{json, Map, Value};
use std::io::{BufRead, Write};
use std::path::Path;
use std::time::{SystemTime, UNIX_EPOCH};

pub const SERVER_NAME: &str = "ocg";
pub const MAX_REQUEST_BYTES: usize = 1024 * 1024;
pub const DEFAULT_MISSION_LIMIT: usize = 50;
pub const MAX_MISSION_LIMIT: usize = 100;
pub const DEFAULT_LIST_LIMIT: usize = 50;
pub const MAX_LIST_LIMIT: usize = 100;
pub const DEFAULT_REPLAY_LIMIT: usize = 10;
pub const MAX_REPLAY_LIMIT: usize = 25;

const LATEST_PROTOCOL_VERSION: &str = "2025-11-25";
const SUPPORTED_PROTOCOL_VERSIONS: [&str; 4] = [
    "2024-11-05",
    "2025-03-26",
    "2025-06-18",
    LATEST_PROTOCOL_VERSION,
];

/// Run one sequential, bounded MCP session until EOF or an `exit` notification.
pub fn serve_stdio(root: &Path) -> Result<()> {
    let service = ControlService::open(root)?;
    let stdin = std::io::stdin();
    let stdout = std::io::stdout();
    serve(&service, stdin.lock(), stdout.lock())
}

/// Protocol engine separated from process I/O for deterministic tests.
pub fn serve<R: BufRead, W: Write>(
    service: &ControlService,
    mut input: R,
    mut output: W,
) -> Result<()> {
    loop {
        let Some(frame) = read_frame(&mut input)? else {
            return Ok(());
        };
        if frame.is_empty() {
            continue;
        }
        let request: Value = match serde_json::from_slice(&frame) {
            Ok(value) => value,
            Err(_) => {
                write_message(
                    &mut output,
                    protocol_error(Value::Null, -32700, "parse error"),
                )?;
                continue;
            }
        };
        let Some(object) = request.as_object() else {
            write_message(
                &mut output,
                protocol_error(Value::Null, -32600, "invalid request"),
            )?;
            continue;
        };
        let id = object.get("id").cloned();
        let valid_id = id
            .as_ref()
            .is_none_or(|value| value.is_string() || value.is_number());
        if object.get("jsonrpc") != Some(&Value::String("2.0".to_string())) || !valid_id {
            write_message(
                &mut output,
                protocol_error(Value::Null, -32600, "invalid request"),
            )?;
            continue;
        }
        let Some(method) = object.get("method").and_then(Value::as_str) else {
            write_message(
                &mut output,
                protocol_error(id.unwrap_or(Value::Null), -32600, "invalid request"),
            )?;
            continue;
        };
        if method == "exit" && id.is_none() {
            return Ok(());
        }
        if id.is_none() {
            // Notifications (`notifications/initialized`, cancellation, etc.)
            // never receive a response and never mutate authority.
            continue;
        }
        let id = id.expect("checked above");
        let response = match method {
            "initialize" => initialize(object.get("params")),
            "ping" => Ok(json!({})),
            "tools/list" => Ok(json!({ "tools": tool_definitions() })),
            "tools/call" => call_tool(service, object.get("params")),
            "shutdown" => Ok(Value::Null),
            _ => Err(protocol_error(id.clone(), -32601, "method not found")),
        };
        match response {
            Ok(result) => write_message(
                &mut output,
                json!({ "jsonrpc": "2.0", "id": id, "result": result }),
            )?,
            Err(error) => write_message(&mut output, error)?,
        }
    }
}

fn read_frame<R: BufRead>(input: &mut R) -> Result<Option<Vec<u8>>> {
    let mut frame = Vec::new();
    loop {
        let available = input
            .fill_buf()
            .map_err(|error| OcgError::io("cannot read MCP stdin", error))?;
        if available.is_empty() {
            return if frame.is_empty() {
                Ok(None)
            } else {
                Ok(Some(frame))
            };
        }
        let newline = available.iter().position(|byte| *byte == b'\n');
        let take = newline.map_or(available.len(), |index| index + 1);
        if frame.len().saturating_add(take) > MAX_REQUEST_BYTES {
            input.consume(take);
            while newline.is_none() {
                let available = input
                    .fill_buf()
                    .map_err(|error| OcgError::io("cannot drain oversized MCP request", error))?;
                if available.is_empty() {
                    break;
                }
                let next = available.iter().position(|byte| *byte == b'\n');
                let consume = next.map_or(available.len(), |index| index + 1);
                input.consume(consume);
                if next.is_some() {
                    break;
                }
            }
            return Ok(Some(b"{".to_vec()));
        }
        frame.extend_from_slice(&available[..take]);
        input.consume(take);
        if newline.is_some() {
            while matches!(frame.last(), Some(b'\n' | b'\r')) {
                frame.pop();
            }
            return Ok(Some(frame));
        }
    }
}

fn write_message<W: Write>(output: &mut W, value: Value) -> Result<()> {
    serde_json::to_writer(&mut *output, &value)
        .map_err(|error| OcgError::config(format!("cannot serialize MCP response: {error}")))?;
    output
        .write_all(b"\n")
        .and_then(|_| output.flush())
        .map_err(|error| OcgError::io("cannot write MCP stdout", error))
}

fn initialize(params: Option<&Value>) -> std::result::Result<Value, Value> {
    let requested = params
        .and_then(Value::as_object)
        .and_then(|params| params.get("protocolVersion"))
        .and_then(Value::as_str)
        .unwrap_or(LATEST_PROTOCOL_VERSION);
    let negotiated = if SUPPORTED_PROTOCOL_VERSIONS.contains(&requested) {
        requested
    } else {
        LATEST_PROTOCOL_VERSION
    };
    Ok(json!({
        "protocolVersion": negotiated,
        "capabilities": { "tools": { "listChanged": false } },
        "serverInfo": { "name": SERVER_NAME, "version": env!("CARGO_PKG_VERSION") },
        "instructions": "Local, project-scoped OCG control-plane tools. Mutations use ControlService authority."
    }))
}

fn protocol_error(id: Value, code: i64, message: &str) -> Value {
    json!({ "jsonrpc": "2.0", "id": id, "error": { "code": code, "message": message } })
}

fn call_tool(
    service: &ControlService,
    params: Option<&Value>,
) -> std::result::Result<Value, Value> {
    let Some(params) = params.and_then(Value::as_object) else {
        return Ok(tool_error(
            "invalid_argument",
            "tools/call params must be an object",
        ));
    };
    if params
        .keys()
        .any(|key| key != "name" && key != "arguments" && key != "_meta")
    {
        return Ok(tool_error(
            "invalid_argument",
            "unexpected tools/call field",
        ));
    }
    let Some(name) = params.get("name").and_then(Value::as_str) else {
        return Ok(tool_error("invalid_argument", "tool name must be a string"));
    };
    let arguments = match params.get("arguments") {
        None | Some(Value::Null) => Map::new(),
        Some(Value::Object(arguments)) => arguments.clone(),
        Some(_) => {
            return Ok(tool_error(
                "invalid_argument",
                "tool arguments must be an object",
            ))
        }
    };
    let outcome = dispatch_tool(service, name, &arguments);
    Ok(match outcome {
        Ok(value) => tool_success(value),
        Err(error) => tool_error_value(error),
    })
}

fn dispatch_tool(
    service: &ControlService,
    name: &str,
    arguments: &Map<String, Value>,
) -> std::result::Result<Value, McpError> {
    match name {
        "ocg_state_summary" => {
            exact_keys(arguments, &[])?;
            serialized(service.state_summary()?)
        }
        "ocg_mission_list" => {
            exact_keys(arguments, &["limit"])?;
            let limit =
                optional_usize(arguments, "limit", DEFAULT_MISSION_LIMIT, MAX_MISSION_LIMIT)?;
            serialized(service.missions(limit)?)
        }
        "ocg_mission_get" => {
            exact_keys(arguments, &["mission_id"])?;
            serialized(service.mission(safe_id(arguments, "mission_id")?)?)
        }
        "ocg_events_replay" => {
            exact_keys(arguments, &["epoch", "after", "limit"])?;
            let epoch = required_u64(arguments, "epoch")?;
            if epoch == 0 {
                return Err(McpError::invalid("epoch must be positive"));
            }
            let after = required_u64(arguments, "after")?;
            let limit = optional_usize(arguments, "limit", DEFAULT_REPLAY_LIMIT, MAX_REPLAY_LIMIT)?;
            let mut events = match service.replay(epoch, after)? {
                ReplaySlice::Empty => Vec::new(),
                ReplaySlice::Events(events) => events,
            };
            // `ControlService::replay` reads one authority document. Its full
            // returned suffix ends at that document's head; derive the current
            // cursor from it rather than racing a second head read.
            let current_cursor = events
                .last()
                .map(|event| event.cursor)
                .unwrap_or(crate::orchestration::replay::Cursor { epoch, seq: after });
            let has_more = events.len() > limit;
            events.truncate(limit);
            let last_returned_cursor = events.last().map(|event| event.cursor);
            Ok(json!({
                "request_cursor": { "epoch": epoch, "seq": after },
                "current_cursor": current_cursor,
                "events": events,
                "last_returned_cursor": last_returned_cursor,
                "has_more": has_more
            }))
        }
        "ocg_approvals_list" => {
            exact_keys(arguments, &["limit"])?;
            let limit = optional_usize(arguments, "limit", DEFAULT_LIST_LIMIT, MAX_LIST_LIMIT)?;
            serialized(service.authoritative_approvals(limit)?)
        }
        "ocg_resources_list" => {
            exact_keys(arguments, &["limit"])?;
            let limit = optional_usize(arguments, "limit", DEFAULT_LIST_LIMIT, MAX_LIST_LIMIT)?;
            serialized(service.authoritative_resources(limit)?)
        }
        "ocg_budget_get" => {
            exact_keys(arguments, &["mission_id"])?;
            serialized(service.budget(safe_id(arguments, "mission_id")?)?)
        }
        "ocg_approval_approve" | "ocg_approval_reject" => {
            exact_keys(arguments, &["approval_id", "note"])?;
            let approval_id = safe_id(arguments, "approval_id")?;
            let note = optional_note(arguments)?;
            let status = if name == "ocg_approval_approve" {
                ApprovalStatus::Approved
            } else {
                ApprovalStatus::Rejected
            };
            let (approval, cursor) =
                service.resolve_approval(approval_id, status, note, now_unix())?;
            Ok(
                json!({ "outcome": approval.status.as_str(), "approval": approval, "cursor": cursor }),
            )
        }
        "ocg_budget_set" => {
            exact_keys(arguments, &["mission_id", "limit_micros", "currency"])?;
            let mission_id = safe_id(arguments, "mission_id")?;
            let limit_micros = required_i64(arguments, "limit_micros")?;
            if limit_micros <= 0 {
                return Err(McpError::invalid("limit_micros must be positive"));
            }
            let currency = required_string(arguments, "currency")?;
            let currency = normalize_currency(currency)
                .map_err(|error| McpError::invalid(error.to_string()))?;
            serialized(service.set_budget(
                mission_id,
                Money::new(limit_micros, currency),
                now_unix(),
            )?)
        }
        _ => Err(McpError::new("unsupported", "unknown MCP tool")),
    }
}

fn serialized<T: serde::Serialize>(value: T) -> std::result::Result<Value, McpError> {
    serde_json::to_value(value)
        .map_err(|_| McpError::new("internal", "response serialization failed"))
}

fn exact_keys(
    arguments: &Map<String, Value>,
    allowed: &[&str],
) -> std::result::Result<(), McpError> {
    if let Some(key) = arguments
        .keys()
        .find(|key| !allowed.contains(&key.as_str()))
    {
        return Err(McpError::invalid(format!("unexpected argument '{key}'")));
    }
    Ok(())
}

fn required_string<'a>(
    arguments: &'a Map<String, Value>,
    key: &str,
) -> std::result::Result<&'a str, McpError> {
    arguments
        .get(key)
        .and_then(Value::as_str)
        .ok_or_else(|| McpError::invalid(format!("{key} is required and must be a string")))
}

fn safe_id<'a>(
    arguments: &'a Map<String, Value>,
    key: &str,
) -> std::result::Result<&'a str, McpError> {
    let value = required_string(arguments, key)?;
    if !is_safe_id(value) {
        return Err(McpError::invalid(format!("{key} is not a safe identifier")));
    }
    Ok(value)
}

fn required_u64(arguments: &Map<String, Value>, key: &str) -> std::result::Result<u64, McpError> {
    arguments.get(key).and_then(Value::as_u64).ok_or_else(|| {
        McpError::invalid(format!(
            "{key} is required and must be a non-negative integer"
        ))
    })
}

fn required_i64(arguments: &Map<String, Value>, key: &str) -> std::result::Result<i64, McpError> {
    arguments
        .get(key)
        .and_then(Value::as_i64)
        .ok_or_else(|| McpError::invalid(format!("{key} is required and must be an integer")))
}

fn optional_usize(
    arguments: &Map<String, Value>,
    key: &str,
    default: usize,
    maximum: usize,
) -> std::result::Result<usize, McpError> {
    match arguments.get(key) {
        None => Ok(default),
        Some(value) => value
            .as_u64()
            .and_then(|value| usize::try_from(value).ok())
            .filter(|value| *value > 0 && *value <= maximum)
            .ok_or_else(|| McpError::invalid(format!("{key} must be between 1 and {maximum}"))),
    }
}

fn optional_note(arguments: &Map<String, Value>) -> std::result::Result<Option<String>, McpError> {
    match arguments.get("note") {
        None | Some(Value::Null) => Ok(None),
        Some(Value::String(note)) if note.len() <= MAX_REASON_BYTES => Ok(Some(note.clone())),
        Some(Value::String(_)) => Err(McpError::invalid(format!(
            "note must be at most {MAX_REASON_BYTES} bytes"
        ))),
        Some(_) => Err(McpError::invalid("note must be a string")),
    }
}

fn now_unix() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_secs() as i64)
        .unwrap_or(0)
}

#[derive(Debug)]
struct McpError {
    code: &'static str,
    message: String,
    detail: Option<Value>,
}

impl McpError {
    fn new(code: &'static str, message: impl Into<String>) -> Self {
        Self {
            code,
            message: bounded(message.into()),
            detail: None,
        }
    }

    fn invalid(message: impl Into<String>) -> Self {
        Self::new("invalid_argument", message)
    }
}

impl From<ControlError> for McpError {
    fn from(error: ControlError) -> Self {
        let detail = match &error {
            ControlError::WrongEpoch { expected, got } => {
                Some(json!({ "expected_epoch": expected, "got_epoch": got }))
            }
            ControlError::Expired {
                floor_seq,
                requested_seq,
            } => Some(json!({ "floor_seq": floor_seq, "requested_seq": requested_seq })),
            ControlError::Future {
                head_seq,
                requested_seq,
            } => Some(json!({ "head_seq": head_seq, "requested_seq": requested_seq })),
            _ => None,
        };
        let code = match error {
            ControlError::Invalid { .. } => "invalid_argument",
            _ => error.code(),
        };
        Self {
            code,
            message: bounded(error.message()),
            detail,
        }
    }
}

fn bounded(message: String) -> String {
    let redacted = crate::telemetry::task::redact(&message);
    let maximum = crate::orchestration::control::MAX_ERROR_MESSAGE_BYTES;
    if redacted.len() <= maximum {
        return redacted;
    }
    let mut end = maximum;
    while end > 0 && !redacted.is_char_boundary(end) {
        end -= 1;
    }
    format!("{}...", &redacted[..end])
}

fn tool_success(value: Value) -> Value {
    let text = serde_json::to_string(&value).unwrap_or_else(|_| "{}".to_string());
    json!({
        "content": [{ "type": "text", "text": text }],
        "structuredContent": value,
        "isError": false
    })
}

fn tool_error(code: &'static str, message: impl Into<String>) -> Value {
    tool_error_value(McpError::new(code, message))
}

fn tool_error_value(error: McpError) -> Value {
    let mut body = json!({ "error": { "code": error.code, "message": error.message } });
    if let Some(detail) = error.detail {
        body["error"]["detail"] = detail;
    }
    let text = serde_json::to_string(&body).unwrap_or_else(|_| {
        "{\"error\":{\"code\":\"internal\",\"message\":\"serialization failed\"}}".to_string()
    });
    json!({
        "content": [{ "type": "text", "text": text }],
        "structuredContent": body,
        "isError": true
    })
}

fn tool_definitions() -> Vec<Value> {
    vec![
        tool(
            "ocg_state_summary",
            "Compact authoritative OCG state and cursor.",
            empty_schema(),
            true,
            true,
        ),
        tool(
            "ocg_mission_list",
            "Bounded authoritative Mission summaries.",
            object_schema(
                json!({"limit":{"type":"integer","minimum":1,"maximum":MAX_MISSION_LIMIT}}),
                &[],
            ),
            true,
            true,
        ),
        tool(
            "ocg_mission_get",
            "Authoritative Mission detail by id.",
            object_schema(json!({"mission_id":id_schema()}), &["mission_id"]),
            true,
            true,
        ),
        tool(
            "ocg_events_replay",
            "Bounded journal replay strictly after an authoritative cursor.",
            object_schema(
                json!({"epoch":{"type":"integer","minimum":1},"after":{"type":"integer","minimum":0},"limit":{"type":"integer","minimum":1,"maximum":MAX_REPLAY_LIMIT}}),
                &["epoch", "after"],
            ),
            true,
            true,
        ),
        tool(
            "ocg_approvals_list",
            "Bounded durable approval state.",
            list_schema(),
            true,
            true,
        ),
        tool(
            "ocg_resources_list",
            "Descriptive authoritative resource observations; Unknown remains Unknown.",
            list_schema(),
            true,
            true,
        ),
        tool(
            "ocg_budget_get",
            "Durable Mission budget accounting.",
            object_schema(json!({"mission_id":id_schema()}), &["mission_id"]),
            true,
            true,
        ),
        tool(
            "ocg_approval_approve",
            "Approve an existing generation-bound approval.",
            approval_schema(),
            false,
            false,
        ),
        tool(
            "ocg_approval_reject",
            "Reject an existing generation-bound approval.",
            approval_schema(),
            false,
            false,
        ),
        tool(
            "ocg_budget_set",
            "Explicitly set a Mission hard budget through CAS authority.",
            object_schema(
                json!({"mission_id":id_schema(),"limit_micros":{"type":"integer","minimum":1},"currency":{"type":"string","minLength":1,"maxLength":12,"pattern":"^[A-Za-z0-9]+$"}}),
                &["mission_id", "limit_micros", "currency"],
            ),
            false,
            false,
        ),
    ]
}

fn tool(
    name: &str,
    description: &str,
    input_schema: Value,
    read_only: bool,
    idempotent: bool,
) -> Value {
    json!({
        "name": name,
        "description": description,
        "inputSchema": input_schema,
        "annotations": {
            "readOnlyHint": read_only,
            "destructiveHint": false,
            "idempotentHint": idempotent,
            "openWorldHint": false
        }
    })
}

fn empty_schema() -> Value {
    object_schema(json!({}), &[])
}

fn object_schema(properties: Value, required: &[&str]) -> Value {
    json!({
        "type": "object",
        "properties": properties,
        "required": required,
        "additionalProperties": false
    })
}

fn id_schema() -> Value {
    json!({ "type": "string", "minLength": 1, "maxLength": 128, "pattern": "^[a-z0-9-]+$" })
}

fn approval_schema() -> Value {
    object_schema(
        json!({
            "approval_id": id_schema(),
            "note": { "type": "string", "maxLength": MAX_REASON_BYTES }
        }),
        &["approval_id"],
    )
}

fn list_schema() -> Value {
    object_schema(
        json!({"limit":{"type":"integer","minimum":1,"maximum":MAX_LIST_LIMIT}}),
        &[],
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tool_surface_is_bounded_and_has_no_escape_hatches() {
        let tools = tool_definitions();
        let names: Vec<&str> = tools
            .iter()
            .filter_map(|tool| tool["name"].as_str())
            .collect();
        assert_eq!(names.len(), 10);
        assert!(!names.iter().any(|name| {
            name.contains("shell")
                || name.contains("file")
                || name.contains("runtime")
                || name.contains("reconcile")
                || name.contains("resource_set")
        }));
        assert!(tools
            .iter()
            .all(|tool| tool["inputSchema"]["additionalProperties"] == false));
    }
}
