//! Anthropic Messages request encoding.
//!
//! Maps OCG's canonical provider request — an OCG-shaped JSON object produced by
//! canonical launch and by the provider loop — onto the Anthropic Messages wire
//! request. The canonical request keeps one shape for every protocol; only this
//! translation knows what Anthropic calls the same thing.

use serde_json::{json, Map, Value};

use crate::error::{OcgError, Result};

/// Output-token ceiling used when the canonical request declares none.
///
/// The Messages API requires `max_tokens`, so a request that omits it still has
/// to be answered with a number. This is the provider adapter's own default for
/// that required field, not a canonical default: the canonical surface stays
/// optional and every provider decides its own fallback.
pub const DEFAULT_MAX_TOKENS: u32 = 4096;

/// Encode one canonical provider request as an Anthropic Messages request.
///
/// `model` overrides the canonical `model` field: the frozen upstream model id
/// is the wire identity, and it is authoritative over whatever the payload says.
pub fn build_request(canonical: &Value, model: &str, streaming: bool) -> Result<Value> {
    let mut request = Map::new();
    request.insert("model".to_string(), Value::String(model.to_string()));
    request.insert("max_tokens".to_string(), json!(output_ceiling(canonical)?));
    request.insert("stream".to_string(), Value::Bool(streaming));

    let messages = canonical
        .get("messages")
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default();
    let mut encoded = Vec::with_capacity(messages.len());
    let mut system = Vec::new();
    for message in &messages {
        if let Some(turn) = encode_message(message, &mut system)? {
            encoded.push(turn);
        }
    }
    // The Messages API takes the system prompt out of band rather than as a
    // turn, so a leading system turn is lifted out here.
    if !system.is_empty() {
        request.insert("system".to_string(), Value::Array(system));
    }
    // Anthropic rejects a conversation whose final turn is not `user`; the tool
    // loop always ends on a tool result, which already renders as one, so this
    // is a shape guard rather than a repair of the canonical conversation.
    if !encoded.is_empty() {
        request.insert("messages".to_string(), Value::Array(merge_roles(encoded)));
    }

    copy_number(canonical, "temperature", &mut request);
    copy_number(canonical, "top_p", &mut request);
    copy_number(canonical, "top_k", &mut request);
    if let Some(stop) = canonical.get("stop") {
        let sequences = match stop {
            Value::String(one) => vec![one.clone()],
            Value::Array(many) => many
                .iter()
                .filter_map(Value::as_str)
                .map(str::to_string)
                .collect(),
            _ => Vec::new(),
        };
        if !sequences.is_empty() {
            request.insert(
                "stop_sequences".to_string(),
                Value::Array(sequences.into_iter().map(Value::String).collect()),
            );
        }
    }
    if let Some(tools) = canonical.get("tools").and_then(Value::as_array) {
        if !tools.is_empty() {
            let encoded = tools.iter().filter_map(encode_tool).collect::<Vec<_>>();
            if !encoded.is_empty() {
                request.insert("tools".to_string(), Value::Array(encoded));
            }
        }
    }
    if let Some(choice) = canonical.get("tool_choice") {
        if let Some(choice) = encode_tool_choice(choice) {
            request.insert("tool_choice".to_string(), choice);
        }
    }
    // `reasoning_effort` is deliberately not mapped. Anthropic expresses
    // extended thinking as a token budget, and OCG's canonical effort ladder
    // carries no token count, so any mapping would invent a number the caller
    // never chose. Only the quantity the API actually takes is forwarded, and
    // only when the caller states it.
    if let Some(budget) = canonical
        .get("thinking_budget_tokens")
        .and_then(Value::as_u64)
        .filter(|budget| *budget > 0)
    {
        let max_tokens = output_ceiling(canonical)?;
        // The budget is carved out of the same ceiling, so the two can never
        // disagree; a budget at or above it leaves no room for an answer.
        let budget = u32::try_from(budget)
            .unwrap_or(u32::MAX)
            .min(max_tokens.saturating_sub(1));
        if budget > 0 {
            request.insert(
                "thinking".to_string(),
                json!({"type": "enabled", "budget_tokens": budget}),
            );
        }
    }
    Ok(Value::Object(request))
}

/// The Messages output ceiling for this request.
fn output_ceiling(canonical: &Value) -> Result<u32> {
    for field in ["max_tokens", "max_completion_tokens"] {
        if let Some(value) = canonical.get(field) {
            let value = value.as_u64().filter(|value| *value > 0).ok_or_else(|| {
                OcgError::config(format!("provider request `{field}` must be a token count"))
            })?;
            return u32::try_from(value).map_err(|_| {
                OcgError::config(format!("provider request `{field}` exceeds the wire range"))
            });
        }
    }
    Ok(DEFAULT_MAX_TOKENS)
}

fn copy_number(canonical: &Value, field: &str, request: &mut Map<String, Value>) {
    if let Some(value) = canonical.get(field).and_then(Value::as_f64) {
        request.insert(field.to_string(), json!(value));
    }
}

/// Encode one canonical message, appending to either the system prompt or the
/// conversation. `None` means the message carried nothing encodable.
fn encode_message(message: &Value, system: &mut Vec<Value>) -> Result<Option<Value>> {
    let role = message
        .get("role")
        .and_then(Value::as_str)
        .unwrap_or("user");
    let content = message.get("content");
    match role {
        "system" | "developer" => {
            for text in text_parts(content) {
                system.push(json!({"type": "text", "text": text}));
            }
            Ok(None)
        }
        "tool" => {
            // A canonical tool result becomes one `tool_result` block on a user
            // turn: that is the only shape the Messages API accepts results in.
            let tool_use_id = message
                .get("tool_call_id")
                .and_then(Value::as_str)
                .ok_or_else(|| {
                    OcgError::config("provider tool result is missing its tool_call_id")
                })?;
            Ok(Some(json!({"role": "user", "content": [{
                "type": "tool_result",
                "tool_use_id": tool_use_id,
                "content": rendered_text(content),
            }]})))
        }
        "assistant" => {
            let mut blocks = Vec::new();
            for text in text_parts(content) {
                blocks.push(json!({"type": "text", "text": text}));
            }
            blocks.extend(image_parts(content)?);
            // Extended thinking is not replayed. Anthropic validates a thinking
            // block against its signature, which the canonical assistant message
            // does not carry, so forwarding the reasoning text alone would be
            // rejected. The reasoning is still surfaced to the Chat as canonical
            // reasoning output; it is simply not replayed upstream.
            for call in message
                .get("tool_calls")
                .and_then(Value::as_array)
                .unwrap_or(&Vec::new())
            {
                let id = call
                    .get("id")
                    .and_then(Value::as_str)
                    .ok_or_else(|| OcgError::config("provider tool call is missing its id"))?;
                let function = call.get("function").ok_or_else(|| {
                    OcgError::config("provider tool call is missing its function")
                })?;
                let name = function
                    .get("name")
                    .and_then(Value::as_str)
                    .ok_or_else(|| OcgError::config("provider tool call is missing its name"))?;
                let arguments = function.get("arguments");
                let input = match arguments {
                    // The canonical wire carries arguments as a JSON string;
                    // Messages carries them as an object.
                    Some(Value::String(text)) => serde_json::from_str(text).map_err(|error| {
                        OcgError::config(format!("provider tool call arguments: {error}"))
                    })?,
                    Some(value) => value.clone(),
                    None => Value::Object(Map::new()),
                };
                blocks.push(json!({
                    "type": "tool_use", "id": id, "name": name, "input": input,
                }));
            }
            if blocks.is_empty() {
                return Ok(None);
            }
            Ok(Some(json!({"role": "assistant", "content": blocks})))
        }
        _ => {
            let mut blocks = Vec::new();
            for text in text_parts(content) {
                blocks.push(json!({"type": "text", "text": text}));
            }
            blocks.extend(image_parts(content)?);
            if blocks.is_empty() {
                return Ok(None);
            }
            Ok(Some(json!({"role": "user", "content": blocks})))
        }
    }
}

fn image_parts(content: Option<&Value>) -> Result<Vec<Value>> {
    let mut images = Vec::new();
    for part in content.and_then(Value::as_array).into_iter().flatten() {
        if part.get("type").and_then(Value::as_str) != Some("image_url") {
            continue;
        }
        let url = part
            .get("image_url")
            .and_then(|image| image.get("url"))
            .and_then(Value::as_str)
            .ok_or_else(|| OcgError::config("image has no URL"))?;
        if !crate::chat_images::valid_provider_url(url) {
            return Err(OcgError::config("unsupported image URL"));
        }
        let source = if let Some(data) = url.strip_prefix("data:") {
            let (media_type, data) = data
                .split_once(";base64,")
                .ok_or_else(|| OcgError::config("invalid image data URL"))?;
            json!({"type": "base64", "media_type": media_type, "data": data})
        } else {
            json!({"type": "url", "url": url})
        };
        images.push(json!({"type": "image", "source": source}));
    }
    Ok(images)
}

/// The text fragments of a canonical message content field, which is either a
/// string or an array of typed parts.
fn text_parts(content: Option<&Value>) -> Vec<String> {
    match content {
        Some(Value::String(text)) if !text.is_empty() => vec![text.clone()],
        Some(Value::Array(parts)) => parts
            .iter()
            .filter(|part| part.get("type").and_then(Value::as_str) == Some("text"))
            .filter_map(|part| part.get("text").and_then(Value::as_str))
            .filter(|text| !text.is_empty())
            .map(str::to_string)
            .collect(),
        _ => Vec::new(),
    }
}

/// The single text a block-shaped content field renders as. Tool results are
/// JSON documents, so this is the serialized value rather than a text part.
fn rendered_text(content: Option<&Value>) -> Value {
    match content {
        Some(Value::String(text)) => Value::String(text.clone()),
        Some(value) => Value::String(value.to_string()),
        None => Value::String(String::new()),
    }
}

/// Combine adjacent turns that share a role.
///
/// One round can produce several tool results, each of which renders as its own
/// user turn; the Messages API takes them as one.
fn merge_roles(messages: Vec<Value>) -> Vec<Value> {
    let mut merged: Vec<Value> = Vec::with_capacity(messages.len());
    for message in messages {
        let role = message
            .get("role")
            .and_then(Value::as_str)
            .unwrap_or("user");
        let blocks = message
            .get("content")
            .and_then(Value::as_array)
            .cloned()
            .unwrap_or_default();
        let previous_role = merged.last().and_then(|previous| previous.get("role"));
        let appendable = previous_role.and_then(Value::as_str) == Some(role);
        if appendable {
            if let Some(Value::Array(previous_blocks)) = merged
                .last_mut()
                .and_then(|previous| previous.get_mut("content"))
            {
                previous_blocks.extend(blocks);
            }
        } else {
            merged.push(message);
        }
    }
    merged
}

/// Convert one canonical OpenAI function tool into an Anthropic tool.
///
/// Anthropic validates `input_schema` as JSON Schema directly, so the canonical
/// parameter schema is forwarded as-is rather than through a strict-mode
/// projection: nothing was widened on the way out, so nothing has to be
/// unwound on the way back in.
fn encode_tool(tool: &Value) -> Option<Value> {
    let function = tool.get("function").unwrap_or(tool);
    let name = function.get("name").and_then(Value::as_str)?;
    let mut encoded = Map::new();
    encoded.insert("name".to_string(), Value::String(name.to_string()));
    if let Some(description) = function.get("description").and_then(Value::as_str) {
        encoded.insert(
            "description".to_string(),
            Value::String(description.to_string()),
        );
    }
    let schema = function
        .get("parameters")
        .or_else(|| function.get("input_schema"))
        .cloned()
        .unwrap_or_else(|| json!({"type": "object"}));
    encoded.insert("input_schema".to_string(), schema);
    Some(Value::Object(encoded))
}

fn encode_tool_choice(choice: &Value) -> Option<Value> {
    let (mode, name) = match choice {
        Value::String(mode) => (mode.as_str(), None),
        Value::Object(fields) => {
            let mode = fields.get("type").and_then(Value::as_str)?;
            let name = fields
                .get("function")
                .and_then(|function| function.get("name"))
                .and_then(Value::as_str)
                .or_else(|| fields.get("name").and_then(Value::as_str))
                .map(str::to_string);
            (mode, name)
        }
        _ => return None,
    };
    match mode {
        "auto" => Some(json!({"type": "auto"})),
        "none" => Some(json!({"type": "none"})),
        "required" | "any" => Some(json!({"type": "any"})),
        "function" | "tool" => Some(json!({"type": "tool", "name": name?})),
        _ => None,
    }
}
