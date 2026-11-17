pub(crate) mod authorization;
mod configuration;
pub mod execution;
pub mod scheduler;

pub(crate) use configuration::{ConfigurationResolver, bind_prediction};

use crate::db::entities::{
    self, AttachmentId, DevelopmentCandidate, DevelopmentDecisionContext,
    DevelopmentExecutionContext, UnstartedCause, Violation,
};
use crate::db::{
    AttachmentError, DatabaseError, DatabaseHandle, DevelopmentHandoffError,
    PreparedDevelopmentHandoff, classify_unstarted,
};

#[derive(Clone, Debug)]
pub(crate) struct DevelopmentToolOffer {
    pub(crate) wires: Vec<crate::ai::providers::openai_compatible::ToolWire>,
    pub(crate) grant: Option<&'static str>,
    pub(crate) agents_md: Option<String>,
}

#[derive(Clone, Copy, Debug)]
pub(crate) struct DevelopmentToolContext {
    pub(crate) decider: bool,
    pub(crate) nested: bool,
    pub(crate) plan_step: bool,
}

pub(crate) struct DevelopmentResolvedContext {
    pub snapshot: DevelopmentExecutionContext,
    pub images: Vec<(
        AttachmentId,
        crate::ai::providers::openai_compatible::ImageInput,
    )>,
}

#[allow(clippy::large_enum_variant)]
pub(crate) enum DevelopmentResolvedCandidate {
    Generative(DevelopmentResolvedContext),
    Decision(DevelopmentDecisionContext),
}

pub(crate) async fn resolve_development_candidate(
    database: &DatabaseHandle,
    snapshot: DevelopmentCandidate,
) -> Result<DevelopmentResolvedCandidate, DevelopmentHandoffError> {
    use crate::ai::providers::openai_compatible::image::{
        ImageInput, ImageMime, MAX_IMAGE_BYTES, MAX_REQUEST_IMAGE_BYTES, MAX_REQUEST_IMAGES,
    };
    let snapshot = match snapshot {
        DevelopmentCandidate::Generative(snapshot) => snapshot,
        DevelopmentCandidate::Decision(snapshot) => {
            return Ok(DevelopmentResolvedCandidate::Decision(snapshot));
        }
    };
    let unavailable = |detail: &str| DevelopmentHandoffError::NotStarted {
        cause: UnstartedCause::InputUnavailable,
        detail: detail.to_owned(),
    };
    let records: Vec<_> = snapshot
        .covered_inputs
        .iter()
        .flat_map(|turn| turn.attachments.iter())
        .chain(
            snapshot
                .history
                .iter()
                .flat_map(|turn| turn.attachments.iter()),
        )
        .chain(snapshot.attachments.iter())
        .collect();
    if records.len() > MAX_REQUEST_IMAGES {
        return Err(unavailable("context has too many images"));
    }
    let mut total = 0u64;
    let mut images = Vec::with_capacity(records.len());
    for record in records {
        let mime = ImageMime::from_mime(&record.mime)
            .ok_or_else(|| unavailable("context Attachment is not a supported image"))?;
        if record.byte_length > MAX_IMAGE_BYTES as u64 {
            return Err(unavailable("context image exceeds the per-image limit"));
        }
        total = total.saturating_add(record.byte_length);
        if total > MAX_REQUEST_IMAGE_BYTES as u64 {
            return Err(unavailable("context images exceed the request limit"));
        }
        let content =
            database
                .attachment_content(record.id)
                .await
                .map_err(|error| match error {
                    AttachmentError::Database(error) => classify_unstarted(error),
                    _ => unavailable("context Attachment content is unavailable"),
                })?;
        if content.record != *record {
            return Err(unavailable("context Attachment changed after the snapshot"));
        }
        let image = ImageInput::new(mime, content.bytes)
            .map_err(|_| unavailable("context Attachment content is not a valid image"))?;
        images.push((record.id, image));
    }
    Ok(DevelopmentResolvedCandidate::Generative(
        DevelopmentResolvedContext { snapshot, images },
    ))
}

pub(crate) fn prepare_development_handoff(
    resolved: DevelopmentResolvedCandidate,
    offer: Option<&DevelopmentToolOffer>,
) -> Result<PreparedDevelopmentHandoff, DatabaseError> {
    use crate::ai::compact::{
        ContextLimits, ContextNotices, estimate_request_tokens, fit_with_notices,
    };
    use crate::ai::providers::openai_compatible::image::ImageInput;
    let unavailable = || {
        DatabaseError::Rejected(Violation::InvalidValue(
            "handoff",
            "execution context or policy cannot be safely prepared",
        ))
    };
    match resolved {
        DevelopmentResolvedCandidate::Decision(context) => {
            if context.conversation.parent_id.is_some() || offer.is_some() {
                return Err(unavailable());
            }
            let entities::MutableJobContent::Decision { request, .. } = &context.job.input.content
            else {
                return Err(unavailable());
            };
            let (request, conversation) =
                crate::ai::decisive_model::prepare_request(&context.configuration, request)
                    .map_err(|_| unavailable())?;
            Ok(PreparedDevelopmentHandoff::new(
                DevelopmentCandidate::Decision(context),
                Vec::new(),
                request,
                conversation,
                false,
            ))
        }
        DevelopmentResolvedCandidate::Generative(context) => {
            let snapshot = &context.snapshot;
            let frozen = &snapshot.configuration;
            let is_summary = snapshot.is_summary_work();
            let granted = !is_summary
                && frozen
                    .workspace_enabled(snapshot.project.is_some())
                    .map_err(|_| unavailable())?;
            let manages = frozen
                .agent
                .as_ref()
                .is_some_and(|agent| agent.manages_jobs());
            if is_summary {
                if frozen.provider_refusal().is_some() || offer.is_some() {
                    return Err(unavailable());
                }
            } else if frozen.native_execution_refusal().is_some()
                || (granted && offer.is_none_or(|offer| offer.grant.is_none()))
                || (!granted && offer.is_some() && !manages)
                || offer.is_some_and(|offer| offer.wires.is_empty())
            {
                return Err(unavailable());
            }
            if is_summary {
                let mut messages = vec![
                    crate::ai::providers::openai_compatible::ChatMessage::system(
                        "You are a context summarizer. Return ONLY valid JSON matching {\"source\":\"...\",\"summary\":\"...\",\"omitted_details\":[...]}. Do not use tools, Markdown fences, or extra prose.",
                    ),
                ];
                let mut combined = String::new();
                for input in &snapshot.covered_inputs {
                    combined.push_str(&input.text);
                }
                if !combined.is_empty() {
                    messages.push(crate::ai::providers::openai_compatible::ChatMessage::user(
                        combined,
                    ));
                }
                messages.push(crate::ai::providers::openai_compatible::ChatMessage::user(
                    snapshot.job.input.text().to_owned(),
                ));
                let conversation = crate::ai::providers::openai_compatible::ChatConversation {
                    messages,
                    ..Default::default()
                };
                let request = crate::ai::generative_model::request_body(
                    &frozen.llm.config.model,
                    &frozen.llm.config.parameters,
                    &conversation,
                )
                .map_err(|_| unavailable())?;
                return Ok(PreparedDevelopmentHandoff::new(
                    DevelopmentCandidate::Generative(context.snapshot),
                    Vec::new(),
                    request,
                    Some(conversation),
                    false,
                ));
            }
            let mut image_cursor = context.images.iter();
            let mut images_for =
                |records: &[entities::AttachmentRecord]| -> Result<Vec<ImageInput>, DatabaseError> {
                    records
                        .iter()
                        .map(|record| {
                            let (id, image) = image_cursor.next().ok_or_else(unavailable)?;
                            if *id != record.id {
                                return Err(unavailable());
                            }
                            Ok(image.clone())
                        })
                        .collect()
                };
            let mut prior = Vec::new();
            if let Some(summary) = &snapshot.adopted_summary {
                prior.push((Some(format!("Generated context summary (advisory, not an original user instruction). Originals remain available:\n{}", summary.summary.context_text())), Vec::new(), None));
            }
            for input in &snapshot.covered_inputs {
                prior.push((
                    Some(input.text.clone()),
                    images_for(&input.attachments)?,
                    None,
                ));
            }
            for turn in &snapshot.history {
                let answer = if turn
                    .record
                    .outcome
                    .as_ref()
                    .is_some_and(|outcome| outcome.kind == entities::OutcomeKind::Cancelled)
                    || turn.record.job.phase != entities::MutableJobPhase::Executing
                {
                    None
                } else {
                    turn.outputs
                        .last()
                        .and_then(entities::JobOutputRecord::display_text)
                };
                prior.push((
                    Some(turn.record.job.input.text().to_owned()),
                    images_for(&turn.attachments)?,
                    answer,
                ));
            }
            let current_images = images_for(&snapshot.attachments)?;
            if image_cursor.next().is_some() {
                return Err(unavailable());
            }
            let limits = ContextLimits::for_profile(&frozen.llm.config);
            let mut conversation = fit_with_notices(
                frozen,
                prior,
                Some(snapshot.job.input.text()),
                &current_images,
                snapshot.project.as_ref(),
                offer.and_then(|offer| offer.grant),
                offer.and_then(|offer| offer.agents_md.as_deref()),
                offer.map(|offer| offer.wires.clone()).unwrap_or_default(),
                limits,
                ContextNotices::default(),
            )
            .ok_or_else(unavailable)?;
            if offer.is_none_or(|offer| offer.grant.is_none())
                && let Some(project) = &snapshot.project
            {
                let first = conversation.messages.first_mut().ok_or_else(unavailable)?;
                *first = crate::ai::providers::openai_compatible::ChatMessage::system(format!(
                    "You work in the Project {:?}. Its workspace root is {}. No workspace access or tools are available for this execution; do not claim to read or change project files.",
                    project.name, project.root
                ));
            }
            if conversation.dropped_turns != 0
                || conversation.tools.len() != offer.map_or(0, |offer| offer.wires.len())
                || estimate_request_tokens(&conversation, limits.max_tokens)
                    > u64::from(limits.context_window.ok_or_else(unavailable)?)
            {
                return Err(unavailable());
            }
            let request = crate::ai::generative_model::request_body(
                &frozen.llm.config.model,
                &frozen.llm.config.parameters,
                &conversation,
            )
            .map_err(|_| unavailable())?;
            let mut verified = context.images.iter();
            let messages = request
                .get("messages")
                .and_then(serde_json::Value::as_array)
                .ok_or_else(unavailable)?;
            for (index, message) in messages.iter().enumerate() {
                let mut ids = Vec::new();
                if let Some(parts) = message.get("content").and_then(serde_json::Value::as_array) {
                    for part in parts.iter().filter(|part| {
                        part.get("type").and_then(serde_json::Value::as_str) == Some("image_url")
                    }) {
                        let (id, image) = verified.next().ok_or_else(unavailable)?;
                        if serde_json::to_value(image.part()).map_err(|_| unavailable())? != *part {
                            return Err(unavailable());
                        }
                        ids.push(*id);
                    }
                }
                if !ids.is_empty() {
                    conversation.image_attachments.insert(index, ids);
                }
            }
            if verified.next().is_some() {
                return Err(unavailable());
            }
            Ok(PreparedDevelopmentHandoff::new(
                DevelopmentCandidate::Generative(context.snapshot),
                context.images.into_iter().map(|(id, _)| id).collect(),
                request,
                Some(conversation),
                offer.is_some(),
            ))
        }
    }
}
