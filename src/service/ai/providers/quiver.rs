use super::*;

pub(crate) const DEFAULT_ENDPOINT: &str = "https://api.quiver.ai/v1";

pub(crate) fn request_body(chat: &Value) -> Result<Value, ProviderExecutionError> {
    let fail = |message: &str| ProviderExecutionError::Build {
        message: message.to_owned(),
    };
    if chat.get("tools").is_some()
        || chat.get("tool_choice").is_some()
        || chat.get("response_format").is_some()
        || chat.get("parallel_tool_calls").is_some()
    {
        return Err(fail(
            "Quiver SVG generation does not support tools or structured responses",
        ));
    }
    if chat.get("frequency_penalty").is_some() {
        return Err(fail(
            "Quiver SVG generation does not support frequency_penalty",
        ));
    }
    if chat.get("max_tokens").is_some() && chat.get("max_completion_tokens").is_some() {
        return Err(fail(
            "max_tokens and max_completion_tokens are mutually exclusive",
        ));
    }
    let mut prompt = None;
    let mut instructions = Vec::new();
    let messages = chat
        .get("messages")
        .and_then(Value::as_array)
        .ok_or_else(|| fail("request has no messages"))?;
    for message in messages {
        let role = message.get("role").and_then(Value::as_str);
        let text = message
            .get("content")
            .and_then(Value::as_str)
            .ok_or_else(|| fail("Quiver SVG generation requires text-only messages"))?;
        match role {
            Some("system" | "developer") if prompt.is_none() => instructions.push(text),
            Some("user") if prompt.is_none() => prompt = Some(text),
            _ => {
                return Err(fail(
                    "Quiver SVG generation requires a single user message and does not support conversation history",
                ));
            }
        }
    }
    let prompt = prompt
        .filter(|text| !text.trim().is_empty() && text.chars().count() <= 10_000)
        .ok_or_else(|| fail("Quiver SVG prompt must contain 1 to 10000 characters"))?;
    let mut body = json!({"model": chat["model"], "prompt": prompt, "stream": false});
    if !instructions.is_empty() {
        body["instructions"] = json!(instructions.join("\n\n"));
    }
    for key in [
        "temperature",
        "top_p",
        "presence_penalty",
        "reasoning_effort",
    ] {
        if let Some(value) = chat.get(key) {
            body[key] = value.clone();
        }
    }
    if let Some(value) = chat
        .get("max_tokens")
        .or_else(|| chat.get("max_completion_tokens"))
    {
        body["max_output_tokens"] = value.clone();
    }
    Ok(body)
}
