pub(crate) mod admission;
pub mod error;
pub mod image;
pub mod normalize;
pub mod request;
pub mod stream;

pub use error::{ProviderFailure, TransportError};
pub use image::{ImageError, ImageInput, ImageMime};
pub use request::{
    ChatConversation, ChatFunctionCallWire, ChatMessage, ChatRequest, ChatRole, ChatToolCallWire,
    ContentPartWire, FunctionDefinitionWire, ImageUrlWire, JsonSchemaWire, MAX_FUNCTION_NAME_BYTES,
    MAX_TOOL_ARGUMENTS_BYTES, MAX_TOOL_CALL_ID_BYTES, MAX_TOOL_CALLS, MAX_TOOL_DEFINITION_BYTES,
    MAX_TOOLS, MessageContentWire, NamedFunctionWire, NamedToolChoiceWire, ResponseFormatWire,
    StopSequences, StreamOptions, ToolChoiceWire, ToolWire, is_function_name,
};
pub use stream::{
    ChatFinishReason, ChatStreamEvent, ChatStreamSummary, CompletedToolCall, InvalidToolCall,
    NormalizedUsage, ToolCallAssembler, ToolCallRefusal,
};

pub(crate) const DEFAULT_ENDPOINT: &str = "https://api.openai.com/v1/chat/completions";
