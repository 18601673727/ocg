//! Thin MCP protocol glue: schema types, session correlation and policy.
//! Transport ownership stays with ntex and execution ownership stays with
//! Compio; this module does not implement another protocol runtime.

use crate::error::{OcgError, Result};
use rust_mcp_schema::mcp_2025_11_25::{
    JsonrpcErrorResponse, JsonrpcMessage, JsonrpcRequest, JsonrpcResponse, RequestId, RpcError,
};
use serde_json::{Map, Value};
use std::io::{BufRead, Write};
use std::sync::atomic::{AtomicU64, Ordering};

fn invalid(message: &str) -> OcgError {
    OcgError::config(message)
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct McpSession {
    pub session_id: String,
    pub initialized: bool,
}

impl McpSession {
    pub fn new(session_id: impl Into<String>) -> Result<Self> {
        let session_id = session_id.into();
        if session_id.is_empty() || session_id.len() > 160 {
            return Err(invalid("invalid MCP session id"));
        }
        Ok(Self {
            session_id,
            initialized: false,
        })
    }

    pub fn initialize(&mut self) {
        self.initialized = true;
    }
}

#[derive(Debug, Clone)]
pub struct McpEnvelope {
    pub session_id: String,
    pub request: JsonrpcRequest,
}

pub trait McpDispatcher: Send + Sync {
    fn dispatch(&self, session: &McpSession, request: JsonrpcRequest) -> Result<JsonrpcResponse>;
}

pub fn decode(session: &McpSession, body: &[u8]) -> Result<McpEnvelope> {
    if body.len() > 1024 * 1024 {
        return Err(invalid("MCP request exceeds size limit"));
    }
    let request: JsonrpcRequest = serde_json::from_slice(body)
        .map_err(|error| invalid(&format!("invalid MCP JSON-RPC request: {error}")))?;
    Ok(McpEnvelope {
        session_id: session.session_id.clone(),
        request,
    })
}

pub fn decode_message(body: &[u8]) -> Result<JsonrpcMessage> {
    if body.len() > 1024 * 1024 {
        return Err(invalid("MCP request exceeds size limit"));
    }
    serde_json::from_slice(body)
        .map_err(|error| invalid(&format!("invalid MCP JSON-RPC message: {error}")))
}

pub fn encode_message(message: &JsonrpcMessage) -> Result<Vec<u8>> {
    serde_json::to_vec(message)
        .map_err(|error| invalid(&format!("serialize MCP JSON-RPC message: {error}")))
}

pub fn error_message(id: Option<RequestId>, code: i64, message: &str) -> JsonrpcMessage {
    JsonrpcMessage::ErrorResponse(JsonrpcErrorResponse::new(
        RpcError {
            code,
            data: None,
            message: message.to_string(),
        },
        id,
    ))
}

/// STDIO framing is transport glue, not protocol parsing. The payload itself
/// is decoded exclusively through rust-mcp-schema by `decode_message`.
pub fn read_stdio_frame<R: BufRead>(input: &mut R) -> Result<Option<Vec<u8>>> {
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
        if frame.len().saturating_add(take) > 1024 * 1024 {
            input.consume(take);
            while newline.is_none() {
                let (consume, found_newline) = {
                    let available = input.fill_buf().map_err(|error| {
                        OcgError::io("cannot drain oversized MCP request", error)
                    })?;
                    if available.is_empty() {
                        (0, false)
                    } else {
                        let next = available.iter().position(|byte| *byte == b'\n');
                        (
                            next.map_or(available.len(), |index| index + 1),
                            next.is_some(),
                        )
                    }
                };
                if consume == 0 {
                    break;
                }
                input.consume(consume);
                if found_newline {
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

pub fn write_stdio_message<W: Write>(output: &mut W, message: &JsonrpcMessage) -> Result<()> {
    let encoded = encode_message(message)?;
    output
        .write_all(&encoded)
        .and_then(|_| output.write_all(b"\n"))
        .and_then(|_| output.flush())
        .map_err(|error| OcgError::io("cannot write MCP stdout", error))
}

pub fn dispatch(
    session: &mut McpSession,
    dispatcher: &dyn McpDispatcher,
    envelope: McpEnvelope,
) -> Result<Vec<u8>> {
    if envelope.session_id != session.session_id {
        return Err(invalid("MCP session correlation mismatch"));
    }
    if envelope.request.method == "initialize" {
        session.initialize();
    } else if !session.initialized {
        return Err(invalid("MCP request arrived before initialize"));
    }
    let response = dispatcher.dispatch(session, envelope.request)?;
    serde_json::to_vec(&response)
        .map_err(|error| invalid(&format!("serialize MCP JSON-RPC response: {error}")))
}

pub fn request_id(value: &JsonrpcRequest) -> RequestId {
    value.id.clone()
}

static NEXT_SESSION: AtomicU64 = AtomicU64::new(1);

pub fn new_session_id() -> String {
    format!("mcp-{}", NEXT_SESSION.fetch_add(1, Ordering::Relaxed))
}

pub fn params_object(value: Option<&Map<String, Value>>) -> Map<String, Value> {
    value.cloned().unwrap_or_default()
}

/// ntex owns HTTP payload framing; this function only collects the bounded
/// body and hands it to the schema/correlation glue.
pub async fn receive_ntex(
    session: &McpSession,
    payload: &mut ntex::web::types::Payload,
) -> Result<McpEnvelope> {
    let mut body = Vec::new();
    while let Some(chunk) = payload.recv().await {
        let chunk = chunk.map_err(|error| invalid(&format!("read MCP ntex payload: {error}")))?;
        if body.len().saturating_add(chunk.len()) > 1024 * 1024 {
            return Err(invalid("MCP request exceeds size limit"));
        }
        body.extend_from_slice(&chunk);
    }
    decode(session, &body)
}

/// Handle one ntex HTTP request while keeping protocol decoding and session
/// correlation in this module. The application dispatcher remains transport
/// neutral and returns the generated rust-mcp-schema response type.
pub async fn handle_ntex<D: McpDispatcher>(
    session: &mut McpSession,
    dispatcher: &D,
    payload: &mut ntex::web::types::Payload,
) -> Result<ntex::web::HttpResponse> {
    let mut body = Vec::new();
    while let Some(chunk) = payload.recv().await {
        let chunk = chunk.map_err(|error| invalid(&format!("read MCP ntex payload: {error}")))?;
        if body.len().saturating_add(chunk.len()) > 1024 * 1024 {
            return Err(invalid("MCP request exceeds size limit"));
        }
        body.extend_from_slice(&chunk);
    }
    let message = decode_message(&body)?;
    match message {
        JsonrpcMessage::Request(request) => {
            let envelope = McpEnvelope {
                session_id: session.session_id.clone(),
                request,
            };
            let bytes = dispatch(session, dispatcher, envelope)?;
            Ok(ntex::web::HttpResponse::Ok()
                .content_type("application/json")
                .body(bytes))
        }
        JsonrpcMessage::Notification(notification) => {
            if notification.method == "initialize" {
                session.initialize();
            } else if !session.initialized && notification.method != "exit" {
                return Err(invalid("MCP notification arrived before initialize"));
            }
            Ok(ntex::web::HttpResponse::NoContent().finish())
        }
        JsonrpcMessage::ResultResponse(_) | JsonrpcMessage::ErrorResponse(_) => Err(invalid(
            "MCP HTTP endpoint accepts requests and notifications only",
        )),
    }
}
