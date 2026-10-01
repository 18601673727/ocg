//! OpenAI-compatible response and SSE encoding.
//!
//! SSE framing for the OpenAI Chat Completions wire format.

use serde_json::{json, Value};
use super::stream::NormalizedUsage;

/// A Chat Completion chunk for SSE streaming.
#[derive(Debug, Clone)]
pub struct ChatCompletionChunk {
    pub id: String,
    pub model: String,
    pub created: i64,
    pub choices: Vec<ChunkChoice>,
    pub usage: Option<Value>,
}

#[derive(Debug, Clone)]
pub struct ChunkChoice {
    pub index: u32,
    pub delta: Value,
    pub finish_reason: Option<&'static str>,
}

/// Encode a delta chunk as an SSE `data:` line.
pub fn encode_chunk(
    id: &str,
    model: &str,
    delta: Value,
    finish: Option<&str>,
    usage: Option<Value>,
) -> Vec<u8> {
    let choices = if usage.is_some() {
        json!([])
    } else {
        json!([{"index":0,"delta":delta,"finish_reason":finish}])
    };
    let mut chunk = json!({"id":id,"object":"chat.completion.chunk","created":now(),"model":model,"choices":choices});
    if let Some(usage) = usage {
        chunk["usage"] = usage;
    }
    format!("data: {chunk}\n\n").into_bytes()
}

/// Encode a usage-only chunk as an SSE `data:` line.
pub fn encode_usage_chunk(
    id: &str,
    model: &str,
    usage: &NormalizedUsage,
) -> Vec<u8> {
    let value = usage.raw.clone().unwrap_or_else(|| {
        json!({
            "prompt_tokens": usage.input_tokens, "completion_tokens": usage.output_tokens,
            "total_tokens": usage.total_tokens(),
        })
    });
    encode_chunk(id, model, json!({}), None, Some(value))
}

/// Write the SSE `[DONE]` sentinel.
pub fn encode_sse_done() -> Vec<u8> {
    b"data: [DONE]\n\n".to_vec()
}

/// Encode the protocol error body without binding it to an HTTP server.
pub fn encode_error(message: &str) -> Vec<u8> {
    json!({"error":{"message":message,"type":"invalid_request_error"}}).to_string().into_bytes()
}

fn now() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs() as i64
}
