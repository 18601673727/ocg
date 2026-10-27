use super::providers::openai_compatible::{
    ChatConversation, ChatMessage, ChatRole, ContentPartWire, ImageInput, MessageContentWire,
    ToolWire,
};
use crate::db::entities::{FrozenExecutionConfig, ProjectRecord};

pub(crate) const IMAGES_OMITTED_NOTICE: &str = "Images of earlier turns were omitted; a request carries at most 8 images and 16 MiB of images.";

pub(crate) const OMITTED_NOTICE: &str =
    "Earlier turns of this conversation were omitted to fit the model's context window.";

fn append_omitted_notice(conversation: &mut ChatConversation) {
    if let Some(sys_msg) = conversation
        .messages
        .iter_mut()
        .find(|m| m.role == ChatRole::System)
    {
        match &mut sys_msg.content {
            Some(MessageContentWire::Text(text)) => {
                if !text.is_empty() && !text.ends_with("\n\n") {
                    if text.ends_with('\n') {
                        text.push('\n');
                    } else {
                        text.push_str("\n\n");
                    }
                }
                text.push_str(OMITTED_NOTICE);
            }
            Some(MessageContentWire::Parts(parts)) => {
                parts.push(ContentPartWire::Text {
                    text: format!("\n\n{OMITTED_NOTICE}"),
                });
            }
            None => {
                sys_msg.content = Some(MessageContentWire::Text(OMITTED_NOTICE.to_owned()));
            }
        }
    } else {
        conversation
            .messages
            .insert(0, ChatMessage::system(OMITTED_NOTICE));
    }
}

fn message_weights(message: &ChatMessage) -> (usize, usize) {
    let mut bytes: usize = 0;
    let mut images: usize = 0;

    if let Some(content) = &message.content {
        match content {
            MessageContentWire::Text(text) => {
                bytes = bytes.saturating_add(text.len());
            }
            MessageContentWire::Parts(parts) => {
                for part in parts {
                    match part {
                        ContentPartWire::Text { text } => {
                            bytes = bytes.saturating_add(text.len());
                        }
                        ContentPartWire::ImageUrl { .. } => {
                            images = images.saturating_add(1);
                        }
                    }
                }
            }
        }
    }
    if let Some(reasoning) = &message.reasoning_content {
        bytes = bytes.saturating_add(reasoning.len());
    }
    if let Some(calls) = &message.tool_calls {
        for call in calls {
            bytes = bytes.saturating_add(call.function.name.len());
            bytes = bytes.saturating_add(call.function.arguments.len());
        }
    }

    (bytes, images)
}

fn tool_bytes(tool: &ToolWire) -> usize {
    serde_json::to_vec(tool).map_or(0, |v| v.len())
}

/// Estimates the token count of a conversation request under conservative assumptions:
/// `ceil(UTF-8 bytes / 3)` over all message text, tool arguments, tool results, and
/// offered tool definitions (serializing each [`ToolWire`]: name, description, and
/// parameters), plus each tool call's function name with its arguments, plus 1,600
/// tokens per image, plus `max_tokens` (or 4,096 when unset).
pub(crate) fn estimate_request_tokens(
    conversation: &ChatConversation,
    max_tokens: Option<u32>,
) -> u64 {
    let mut bytes: usize = 0;
    let mut images: usize = 0;

    for message in &conversation.messages {
        let (m_bytes, m_images) = message_weights(message);
        bytes = bytes.saturating_add(m_bytes);
        images = images.saturating_add(m_images);
    }

    for tool in &conversation.tools {
        bytes = bytes.saturating_add(tool_bytes(tool));
    }

    let text_tokens = bytes.div_ceil(3) as u64;
    let image_tokens = (images as u64).saturating_mul(1600);
    let output_tokens = max_tokens.map_or(4096, u64::from);
    text_tokens
        .saturating_add(image_tokens)
        .saturating_add(output_tokens)
}

pub(crate) const DEFAULT_CONTEXT_WINDOW: u32 = 128_000;

#[derive(Clone, Copy, Default, Debug, PartialEq, Eq)]
pub(crate) struct ContextLimits {
    pub(crate) context_window: Option<u32>,
    pub(crate) max_tokens: Option<u32>,
}

impl ContextLimits {
    pub(crate) fn read_budget(self) -> usize {
        self.context_window
            .unwrap_or(DEFAULT_CONTEXT_WINDOW)
            .saturating_sub(self.max_tokens.unwrap_or(4096)) as usize
    }

    pub(crate) fn for_profile(config: &crate::db::entities::LlmConfig) -> Self {
        Self {
            context_window: config.context_window,
            max_tokens: Some(
                config
                    .parameters
                    .max_completion_tokens
                    .or(config.parameters.max_tokens)
                    .unwrap_or(4096),
            ),
        }
    }
}

pub(crate) struct PriorTurnMessages {
    pub(crate) user: Option<ChatMessage>,
    pub(crate) assistant: Option<ChatMessage>,
}

fn system_messages(
    frozen: &FrozenExecutionConfig,
    project: Option<&ProjectRecord>,
    grant: Option<&str>,
    agents_md: Option<&str>,
    omitted_notice: bool,
) -> Vec<ChatMessage> {
    let mut messages = Vec::new();

    if let Some(project) = project {
        let command_names = crate::db::entities::BUILT_IN_COMMANDS
            .into_iter()
            .chain(
                project
                    .commands
                    .iter()
                    .filter(|command| command.status == crate::db::entities::CommandStatus::Allowed)
                    .map(|command| command.name.as_str()),
            )
            .collect::<Vec<_>>();
        messages.push(super::generative_model::ground_project(
            &project.name,
            &project.root,
            grant,
            &command_names,
        ));
    }

    let mut agent_conv = super::generative_model::objective_conversation(frozen, None);
    agent_conv.messages.pop();

    if omitted_notice {
        append_omitted_notice(&mut agent_conv);
    }

    messages.extend(agent_conv.messages);

    if let Some(agents_md) = agents_md.filter(|content| !content.is_empty()) {
        messages.push(ChatMessage::system(agents_md));
    }

    messages
}

fn assemble_final_conversation(
    system_messages: Vec<ChatMessage>,
    prior_turns: Vec<PriorTurnMessages>,
    current_message: Option<ChatMessage>,
    tools: Vec<ToolWire>,
) -> ChatConversation {
    let mut messages = system_messages;
    for turn in prior_turns {
        if let Some(user) = turn.user {
            messages.push(user);
        }
        if let Some(assistant) = turn.assistant {
            messages.push(assistant);
        }
    }
    if let Some(current) = current_message {
        messages.push(current);
    }
    ChatConversation {
        messages,
        tools,
        ..Default::default()
    }
}

/// What the context read already dropped or says, which the fitted request
/// must carry and count.
#[derive(Clone, Copy, Default)]
pub(crate) struct ContextNotices<'a> {
    pub(crate) images_omitted: bool,
    pub(crate) context_omitted: bool,
    pub(crate) tree_status: Option<&'a str>,
    // Execution appends these messages after fitting the history.
    pub(crate) request_messages: &'a [ChatMessage],
}

#[allow(clippy::too_many_arguments)]
pub(crate) fn fit_with_notices(
    frozen: &FrozenExecutionConfig,
    prior_turns: Vec<(Option<String>, Vec<ImageInput>, Option<String>)>,
    current_objective: Option<&str>,
    current_images: &[ImageInput],
    project: Option<&ProjectRecord>,
    grant: Option<&str>,
    agents_md: Option<&str>,
    tools: Vec<ToolWire>,
    context_limits: ContextLimits,
    notices: ContextNotices<'_>,
) -> Option<ChatConversation> {
    // The system messages of the request, led by the tree status and the
    // images notice; the omitted notice is part of the system prompt.
    let with_prefix = |omitted: bool| {
        let mut system = Vec::new();
        if let Some(status) = notices.tree_status.filter(|status| !status.is_empty()) {
            system.push(ChatMessage::system(status));
        }
        if notices.images_omitted {
            system.push(ChatMessage::system(IMAGES_OMITTED_NOTICE));
        }
        system.extend(system_messages(frozen, project, grant, agents_md, omitted));
        system
    };
    // 1. Build messages once without per-attempt cloning.
    let prior_messages: Vec<PriorTurnMessages> = prior_turns
        .into_iter()
        .map(|(objective, images, assistant_text)| {
            let user = super::generative_model::objective_conversation_with_images(
                frozen,
                objective.as_deref(),
                &images,
            )
            .messages
            .pop();
            let assistant =
                assistant_text.map(|text| ChatMessage::assistant(Some(text), Vec::new()));
            PriorTurnMessages { user, assistant }
        })
        .collect();

    let current_user_message = super::generative_model::objective_conversation_with_images(
        frozen,
        current_objective,
        current_images,
    )
    .messages
    .pop();

    let Some(window) = context_limits.context_window else {
        return Some(assemble_final_conversation(
            with_prefix(notices.context_omitted),
            prior_messages,
            current_user_message,
            tools,
        ));
    };

    let system_without_notice = with_prefix(notices.context_omitted);
    let system_with_notice = with_prefix(true);

    // 2. Estimate token weights once for prior turns and the rest (system, notice, current turn, tools, max_tokens).
    let tools_bytes: usize = tools.iter().map(tool_bytes).sum();
    let request_bytes: usize = notices
        .request_messages
        .iter()
        .map(|message| message_weights(message).0)
        .sum();
    let (current_bytes, current_images_count) = current_user_message
        .as_ref()
        .map_or((0, 0), message_weights);

    let system_without_notice_bytes: usize = system_without_notice
        .iter()
        .map(|m| message_weights(m).0)
        .sum();
    let system_with_notice_bytes: usize = system_with_notice
        .iter()
        .map(|m| message_weights(m).0)
        .sum();

    let rest_without_notice_bytes = system_without_notice_bytes
        .saturating_add(request_bytes)
        .saturating_add(current_bytes)
        .saturating_add(tools_bytes);
    let rest_with_notice_bytes = system_with_notice_bytes
        .saturating_add(request_bytes)
        .saturating_add(current_bytes)
        .saturating_add(tools_bytes);

    let prior_weights: Vec<(usize, usize)> = prior_messages
        .iter()
        .map(|turn| {
            let (user_b, user_img) = turn.user.as_ref().map_or((0, 0), message_weights);
            let (asst_b, asst_img) = turn.assistant.as_ref().map_or((0, 0), message_weights);
            (
                user_b.saturating_add(asst_b),
                user_img.saturating_add(asst_img),
            )
        })
        .collect();

    let total_prior_bytes: usize = prior_weights.iter().map(|(b, _)| *b).sum();
    let total_prior_images: usize = prior_weights.iter().map(|(_, img)| *img).sum();

    let output_tokens = context_limits.max_tokens.map_or(4096, u64::from);

    // 3. Check if full conversation (no turns dropped, no notice) fits.
    let full_bytes = rest_without_notice_bytes.saturating_add(total_prior_bytes);
    let full_images = current_images_count.saturating_add(total_prior_images);
    let full_tokens = (full_bytes.div_ceil(3) as u64)
        .saturating_add((full_images as u64).saturating_mul(1600))
        .saturating_add(output_tokens);

    if full_tokens <= window as u64 {
        return Some(assemble_final_conversation(
            system_without_notice,
            prior_messages,
            current_user_message,
            tools,
        ));
    }

    // 4. Drop oldest turns until the sum fits.
    let total_priors = prior_messages.len();
    let mut remaining_prior_bytes = total_prior_bytes;
    let mut remaining_prior_images = total_prior_images;
    let mut selected_drop = None;

    for drop_count in 1..=total_priors {
        let (dropped_b, dropped_img) = prior_weights[drop_count - 1];
        remaining_prior_bytes = remaining_prior_bytes.saturating_sub(dropped_b);
        remaining_prior_images = remaining_prior_images.saturating_sub(dropped_img);

        let candidate_bytes = rest_with_notice_bytes.saturating_add(remaining_prior_bytes);
        let candidate_images = current_images_count.saturating_add(remaining_prior_images);
        let candidate_tokens = (candidate_bytes.div_ceil(3) as u64)
            .saturating_add((candidate_images as u64).saturating_mul(1600))
            .saturating_add(output_tokens);

        if candidate_tokens <= window as u64 {
            selected_drop = Some(drop_count);
            break;
        }
    }

    // 5. Assemble the final conversation once.
    let drop_count = selected_drop?;
    let mut kept_prior_messages = prior_messages;
    kept_prior_messages.drain(0..drop_count);

    let mut conversation = assemble_final_conversation(
        system_with_notice,
        kept_prior_messages,
        current_user_message,
        tools,
    );
    conversation.dropped_turns = drop_count as u32;
    Some(conversation)
}
