use super::*;

/// Tells the Provider which Project it works in, where its root is, what
/// its grant allows and, for a grant that runs commands, which command names
/// it may run.
pub(crate) fn ground_project(
    name: &str,
    root: &str,
    grant: Option<&str>,
    command_names: &[&str],
) -> ChatMessage {
    let allows = match grant {
        Some("code") => "read, edit and run commands",
        Some("edit") => "read and edit",
        _ => "read",
    };
    let commands = if grant == Some("code") {
        format!(" The commands you may run: {}.", command_names.join(", "))
    } else {
        String::new()
    };
    ChatMessage::system(format!(
        "You work in the Project {name:?}. Its Workspace's absolute root is {root}. Your grant allows: {allows}.{commands} Every tool path is relative to this root. \
         Paths outside this Workspace cannot be reached. If asked to act outside it, \
         say that you cannot reach that path instead of claiming you acted there."
    ))
}

pub async fn execute_chat(
    frozen: &FrozenExecutionConfig,
    objective: Option<&str>,
    resolver: &dyn CredentialResolver,
    config: &ProviderExecutionConfig,
) -> Result<ProviderExecutionResult, ProviderExecutionError> {
    let conversation = objective_conversation(frozen, objective);
    execute_conversation(frozen, &conversation, resolver, config, None).await
}

/// The conversation a Job's objective is sent as: the frozen Agent
/// instructions, when there are any, then the objective. It offers no tools.
#[must_use]
pub fn objective_conversation(
    frozen: &FrozenExecutionConfig,
    objective: Option<&str>,
) -> ChatConversation {
    let mut messages = Vec::new();
    if let Some(instructions) = frozen
        .agent
        .as_ref()
        .and_then(|agent| agent.instructions.as_deref())
        .filter(|instructions| !instructions.is_empty())
    {
        messages.push(ChatMessage::system(instructions));
    }
    messages.push(ChatMessage::user(objective.unwrap_or("")));
    ChatConversation {
        messages,
        ..ChatConversation::default()
    }
}

/// [`objective_conversation`] with `images` in the objective's user message:
/// its text first, when there is any, then each image as an `image_url`
/// part, in order. Without images it is exactly [`objective_conversation`].
#[must_use]
pub fn objective_conversation_with_images(
    frozen: &FrozenExecutionConfig,
    objective: Option<&str>,
    images: &[ImageInput],
) -> ChatConversation {
    let mut conversation = objective_conversation(frozen, objective);
    if images.is_empty() {
        return conversation;
    }
    let text = objective
        .filter(|objective| !objective.is_empty())
        .map(|text| ContentPartWire::Text {
            text: text.to_owned(),
        });
    let parts = text
        .into_iter()
        .chain(images.iter().map(ImageInput::part))
        .collect();
    if let Some(user) = conversation.messages.last_mut() {
        *user = ChatMessage::user_parts(parts);
    }
    conversation
}

/// The request body: the conversation as given, with the frozen model and
/// parameters. Streaming with usage is always requested.
pub(crate) fn request_body(
    model: &str,
    parameters: &LlmParameters,
    conversation: &ChatConversation,
) -> Result<Value, ProviderExecutionError> {
    let mut body =
        serde_json::to_value(conversation).map_err(|error| ProviderExecutionError::Build {
            message: format!("could not encode the chat request: {error}"),
        })?;
    let Some(object) = body.as_object_mut() else {
        return Err(ProviderExecutionError::Build {
            message: "the chat request did not encode as a JSON object".to_owned(),
        });
    };
    object.insert("model".to_owned(), json!(model));
    object.insert("stream".to_owned(), json!(true));
    object.insert("stream_options".to_owned(), json!({"include_usage": true}));
    if let Some(value) = parameters.temperature {
        object.insert("temperature".to_owned(), json!(value));
    }
    if let Some(value) = parameters.top_p {
        object.insert("top_p".to_owned(), json!(value));
    }
    if let Some(value) = parameters.max_tokens {
        object.insert("max_tokens".to_owned(), json!(value));
    }
    if let Some(value) = &parameters.reasoning_effort {
        crate::openai_compatible::request::parse_reasoning_effort(value).map_err(|error| {
            ProviderExecutionError::Build {
                message: error.to_string(),
            }
        })?;
        object.insert("reasoning_effort".to_owned(), json!(value));
    }
    if let Some(value) = parameters.max_completion_tokens {
        object.insert("max_completion_tokens".to_owned(), json!(value));
    }
    if let Some(value) = parameters.presence_penalty {
        object.insert("presence_penalty".to_owned(), json!(value));
    }
    if let Some(value) = parameters.frequency_penalty {
        object.insert("frequency_penalty".to_owned(), json!(value));
    }
    Ok(body)
}
