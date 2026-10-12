//! OpenAI-compatible Chat Completions wire protocol.
//!
//! OCG-owned protocol boundary for OpenAI-compatible HTTP/streaming surfaces.
//! This module owns request parsing, response/stream encoding, and error
//! normalization for the OpenAI Chat Completions wire format.
//!
//! ```text
//! OpenAI-compatible client
//!         ↓
//! OCG-owned ntex HTTP surface
//!         ↓
//! OCG request validation / normalization
//!         ↓
//! Canonical Project / Job / Attempt / Call
//!         ↓
//! OCG provider execution plane
//!         ↓
//! OpenAI-compatible endpoint
//! ```

pub mod error;
pub mod normalize;
pub mod request;
pub mod response;
pub mod stream;

pub use error::{ProviderFailure, TransportError};
pub use normalize::ToolCallNormalizer;
pub use request::{
    ChatFunctionCallWire, ChatMessage, ChatRequest, ChatRole, ChatToolCallWire, ContentPartWire,
    FunctionDefinitionWire, JsonSchemaWire, MessageContentWire, NamedFunctionWire,
    NamedToolChoiceWire, ResponseFormatWire, StopSequences, StreamOptions, ToolChoiceWire,
    ToolWire,
};
pub use response::ChatCompletionChunk;
pub use stream::{
    ChatFinishReason, ChatStreamEvent, ChatStreamSummary, CompletedToolCall, NormalizedUsage,
};
