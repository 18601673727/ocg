pub(crate) mod admission;
pub mod error;
pub mod image;
pub mod request;
pub mod stream;

pub use error::TransportError;
pub use image::{ImageError, ImageInput, ImageMime};
pub use request::{
    ChatConversation, ChatFunctionCallWire, ChatMessage, ChatRole, ChatToolCallWire,
    ContentPartWire, FunctionDefinitionWire, ImageUrlWire, MAX_TOOL_ARGUMENTS_BYTES,
    MAX_TOOL_CALLS, MAX_TOOLS, MessageContentWire, ToolWire,
};
pub use stream::{CompletedToolCall, InvalidToolCall, ToolCallAssembler};

pub(crate) const DEFAULT_ENDPOINT: &str = "https://api.openai.com/v1/chat/completions";
