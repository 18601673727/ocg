use super::providers::anthropic;
use super::providers::openai_compatible::{
    ChatConversation, ChatMessage, ChatRole, ContentPartWire, ImageInput, MessageContentWire,
    ToolWire,
};
use crate::db::entities::{
    DevelopmentContextRequirements, FrozenExecutionConfig, FrozenLlmConfig, LlmParameters,
    LlmTransport, ProjectRecord, VerifiedNativeSummary,
};

pub(super) const ADOPTED_SUMMARY_NOTICE: &str = "Generated context summary (advisory, not an original user instruction). Originals remain available:\n";

pub(crate) const IMAGES_OMITTED_NOTICE: &str = "Images of earlier turns were omitted; a request carries at most 8 images and 16 MiB of images.";

pub(crate) const OMITTED_NOTICE: &str =
    "Earlier turns of this conversation were omitted to fit the model's context window.";

const INTERNET_NOTICE: &str = "Internet access is available during execution through web_search and web_fetch, using OCG's configured proxy. Workspace filesystem path restrictions do not restrict these web tools.";

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

const IMAGE_TOKEN_WEIGHT: u64 = 1600;

fn is_response_image(item: &serde_json::Value) -> bool {
    matches!(
        item.get("type").and_then(serde_json::Value::as_str),
        Some("image" | "input_image" | "image_url")
    )
}

fn response_item_images(items: &[serde_json::Value]) -> usize {
    let mut images: usize = 0;
    for item in items {
        if is_response_image(item) {
            images = images.saturating_add(1);
        }
        if let Some(content) = item["content"].as_array() {
            for part in content {
                if is_response_image(part) {
                    images = images.saturating_add(1);
                }
            }
        }
    }
    images
}

fn message_weights(message: &ChatMessage) -> (usize, usize) {
    if let Some(items) = &message.response_items {
        return (
            crate::db::serialized_len(items).unwrap_or(usize::MAX),
            response_item_images(items),
        );
    }
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
    crate::db::serialized_len(tool).unwrap_or(0)
}

pub(crate) fn estimate_request_tokens(conversation: &ChatConversation, max_tokens: u32) -> u64 {
    ContextEstimate::new(conversation, max_tokens).tokens()
}

/// Incremental form of `estimate_request_tokens` for a conversation that only
/// grows by appended messages: weigh each new message once instead of
/// re-serializing the whole history on every provider round.
pub(crate) struct ContextEstimate {
    bytes: usize,
    images: usize,
    weighed: usize,
    max_tokens: u32,
}

impl ContextEstimate {
    pub(crate) fn new(conversation: &ChatConversation, max_tokens: u32) -> Self {
        let mut estimate = Self {
            bytes: conversation
                .tools
                .iter()
                .map(tool_bytes)
                .fold(0, usize::saturating_add),
            images: 0,
            weighed: 0,
            max_tokens,
        };
        estimate.extend(conversation);
        estimate
    }

    pub(crate) fn extend(&mut self, conversation: &ChatConversation) {
        for message in conversation.messages.iter().skip(self.weighed) {
            let (bytes, images) = message_weights(message);
            self.bytes = self.bytes.saturating_add(bytes);
            self.images = self.images.saturating_add(images);
        }
        self.weighed = conversation.messages.len();
    }

    pub(crate) fn tokens(&self) -> u64 {
        (self.bytes.div_ceil(3) as u64)
            .saturating_add((self.images as u64).saturating_mul(IMAGE_TOKEN_WEIGHT))
            .saturating_add(u64::from(self.max_tokens))
    }
}

pub(crate) const DEFAULT_CONTEXT_WINDOW: u32 = 128_000;

pub(crate) fn reserved_output_tokens(transport: LlmTransport, parameters: &LlmParameters) -> u32 {
    match transport {
        LlmTransport::AnthropicMessages => anthropic::max_tokens(parameters),
        LlmTransport::OpenAiChatCompletions | LlmTransport::OpenAiResponses => parameters
            .max_completion_tokens
            .or(parameters.max_tokens)
            .unwrap_or(4096),
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct ContextLimits {
    pub(crate) context_window: Option<u32>,
    pub(crate) max_tokens: u32,
}

impl ContextLimits {
    pub(crate) fn for_profile(model: &FrozenLlmConfig) -> Self {
        Self {
            context_window: model.config.context_window,
            max_tokens: reserved_output_tokens(model.provider.transport, &model.config.parameters),
        }
    }
}

pub(crate) struct PriorTurnMessages {
    pub(crate) user: Option<ChatMessage>,
    pub(crate) assistant: Vec<ChatMessage>,
}

fn system_messages(
    frozen: &FrozenExecutionConfig,
    project: Option<&ProjectRecord>,
    grant: Option<&str>,
    agents_md: Option<&str>,
    web: bool,
    omitted_notice: bool,
) -> Vec<ChatMessage> {
    let mut messages = Vec::new();

    if web {
        messages.push(ChatMessage::system(INTERNET_NOTICE));
    }

    if let Some(project) = project {
        if matches!(
            frozen.llm.provider.transport,
            LlmTransport::OpenAiResponses | LlmTransport::AnthropicMessages
        ) && grant.is_none()
        {
            messages.push(ChatMessage::system(format!(
                "You work in the Project {:?}. Its workspace root is {}. No workspace access or tools are available for this execution; do not claim to read or change project files.",
                project.name, project.root
            )));
        } else {
            let command_names = crate::db::entities::BUILT_IN_COMMANDS
                .into_iter()
                .chain(
                    project
                        .commands
                        .iter()
                        .filter(|command| {
                            command.status == crate::db::entities::CommandStatus::Allowed
                        })
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
    }
    if frozen.llm.provider.transport == LlmTransport::OpenAiResponses {
        if omitted_notice {
            messages.push(ChatMessage::system(OMITTED_NOTICE));
        }
    } else {
        messages.push(ChatMessage::system(
            super::generative_model::CHART_INSTRUCTIONS,
        ));

        let mut agent_conv = super::generative_model::objective_conversation(frozen, None);
        agent_conv.messages.pop();

        if omitted_notice {
            append_omitted_notice(&mut agent_conv);
        }
        messages.extend(agent_conv.messages);
    }

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
        messages.extend(turn.assistant);
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

#[derive(Clone, Copy, Default)]
pub(crate) struct ContextNotices<'a> {
    pub(crate) images_omitted: bool,
    pub(crate) context_omitted: bool,
    pub(crate) tree_status: Option<&'a str>,
    pub(crate) request_messages: &'a [ChatMessage],
}

#[allow(clippy::too_many_arguments)]
pub(crate) fn fit_with_notices(
    frozen: &FrozenExecutionConfig,
    prior_turns: Vec<(Option<String>, Vec<ImageInput>, Vec<ChatMessage>)>,
    current_objective: Option<&str>,
    current_images: &[ImageInput],
    project: Option<&ProjectRecord>,
    grant: Option<&str>,
    agents_md: Option<&str>,
    tools: Vec<ToolWire>,
    context_limits: ContextLimits,
    notices: ContextNotices<'_>,
) -> Option<ChatConversation> {
    let web = tools.iter().any(|tool| tool.function.name == "web_fetch");
    let with_prefix = |omitted: bool| {
        let mut system = Vec::new();
        if frozen.llm.provider.transport == LlmTransport::OpenAiResponses {
            let mut stable = super::generative_model::CHART_INSTRUCTIONS.to_owned();
            if let Some(instructions) = frozen
                .agent
                .as_ref()
                .and_then(|agent| agent.instructions.as_deref())
                .filter(|text| !text.is_empty())
            {
                stable.push_str("\n\n");
                stable.push_str(instructions);
            }
            system.push(ChatMessage::developer(stable));
        }
        if let Some(status) = notices.tree_status.filter(|status| !status.is_empty()) {
            system.push(ChatMessage::system(status));
        }
        if notices.images_omitted {
            system.push(ChatMessage::system(IMAGES_OMITTED_NOTICE));
        }
        system.extend(system_messages(
            frozen, project, grant, agents_md, web, omitted,
        ));
        system
    };
    let prior_messages: Vec<PriorTurnMessages> = prior_turns
        .into_iter()
        .map(|(objective, images, assistant)| {
            let user = super::generative_model::objective_conversation_with_images(
                frozen,
                objective.as_deref(),
                &images,
            )
            .messages
            .pop();
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
            let (asst_b, asst_img) = turn
                .assistant
                .iter()
                .map(message_weights)
                .fold((0usize, 0usize), |(bytes, images), (b, i)| {
                    (bytes.saturating_add(b), images.saturating_add(i))
                });
            (
                user_b.saturating_add(asst_b),
                user_img.saturating_add(asst_img),
            )
        })
        .collect();

    let total_prior_bytes: usize = prior_weights.iter().map(|(b, _)| *b).sum();
    let total_prior_images: usize = prior_weights.iter().map(|(_, img)| *img).sum();

    let output_tokens = u64::from(context_limits.max_tokens);

    let full_bytes = rest_without_notice_bytes.saturating_add(total_prior_bytes);
    let full_images = current_images_count.saturating_add(total_prior_images);
    let full_tokens = (full_bytes.div_ceil(3) as u64)
        .saturating_add((full_images as u64).saturating_mul(IMAGE_TOKEN_WEIGHT))
        .saturating_add(output_tokens);

    if full_tokens <= window as u64 {
        return Some(assemble_final_conversation(
            system_without_notice,
            prior_messages,
            current_user_message,
            tools,
        ));
    }

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
            .saturating_add((candidate_images as u64).saturating_mul(IMAGE_TOKEN_WEIGHT))
            .saturating_add(output_tokens);

        if candidate_tokens <= window as u64 {
            selected_drop = Some(drop_count);
            break;
        }
    }

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

pub(crate) struct DevelopmentContextBudget;

impl DevelopmentContextRequirements for DevelopmentContextBudget {
    fn max_images(&self) -> usize {
        super::providers::openai_compatible::image::MAX_REQUEST_IMAGES
    }
    fn prompt_bytes(
        &self,
        configuration: &FrozenExecutionConfig,
        project: Option<&ProjectRecord>,
        summary: Option<&VerifiedNativeSummary>,
    ) -> Result<usize, serde_json::Error> {
        let summary_bytes = summary.map_or(0, |summary| {
            summary
                .summary
                .context_text()
                .len()
                .saturating_add(ADOPTED_SUMMARY_NOTICE.len())
        });
        let mut bytes = summary_bytes
            .saturating_add(super::generative_model::CHART_INSTRUCTIONS.len())
            .saturating_add(
                configuration
                    .agent
                    .as_ref()
                    .and_then(|agent| agent.instructions.as_ref())
                    .map_or(0, String::len),
            );
        if let Some(project) = project {
            let commands: Vec<_> = crate::db::entities::BUILT_IN_COMMANDS
                .into_iter()
                .chain(
                    project
                        .commands
                        .iter()
                        .filter(|command| {
                            command.status == crate::db::entities::CommandStatus::Allowed
                        })
                        .map(|command| command.name.as_str()),
                )
                .collect();
            let grant = configuration
                .agent
                .as_ref()
                .and_then(|agent| agent.workspace_grant());
            let grounding = super::generative_model::ground_project(
                &project.name,
                &project.root,
                grant,
                &commands,
            );
            bytes = bytes.saturating_add(crate::db::serialized_len(&grounding)?);
        }
        Ok(bytes)
    }

    fn admits_tokens(
        &self,
        model: &FrozenLlmConfig,
        selected_bytes: usize,
        image_count: usize,
    ) -> bool {
        let limits = ContextLimits::for_profile(model);
        let estimated_tokens = (selected_bytes.div_ceil(3) as u64)
            .saturating_add((image_count as u64).saturating_mul(IMAGE_TOKEN_WEIGHT))
            .saturating_add(u64::from(limits.max_tokens));
        let window = limits.context_window.unwrap_or(DEFAULT_CONTEXT_WINDOW);
        estimated_tokens <= u64::from(window)
    }
}
