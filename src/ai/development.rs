use std::sync::Mutex;

use crate::ai::providers::{
    CancellationToken, RequestRecorder,
    openai_compatible::{ChatConversation, ChatMessage, ContentPartWire, MessageContentWire},
};
use crate::db::entities::{
    self, AttachmentId, DevelopmentCandidate, DevelopmentDecisionContext,
    DevelopmentExecutionContext, Metrics, ProviderAnswer, RequestNumber, Timestamp, UnstartedCause,
    Violation,
};
use crate::db::{
    AttachmentError, DatabaseError, DatabaseHandle, DevelopmentHandoff, DevelopmentHandoffError,
    PreparedDevelopmentHandoff, classify_unstarted,
};

pub(crate) struct PreparedDevelopmentExecution {
    pub(crate) evidence: PreparedDevelopmentHandoff,
    pub(crate) conversation: Option<ChatConversation>,
}

impl PreparedDevelopmentExecution {
    fn new(
        candidate: DevelopmentCandidate,
        image_ids: Vec<AttachmentId>,
        request: serde_json::Value,
        conversation: Option<ChatConversation>,
        offers_tools: bool,
    ) -> Self {
        Self {
            evidence: PreparedDevelopmentHandoff::new(candidate, image_ids, request, offers_tools),
            conversation,
        }
    }
}

pub(crate) struct DevelopmentExecution {
    pub(crate) handoff: DevelopmentHandoff,
    pub(crate) conversation: Option<ChatConversation>,
    recorder: Mutex<RequestRecorder>,
}

impl DevelopmentExecution {
    pub(crate) fn new(handoff: DevelopmentHandoff, conversation: Option<ChatConversation>) -> Self {
        let transport = handoff
            .generative_inputs()
            .map(|(frozen, _, _)| frozen.llm.provider.transport);
        Self {
            handoff,
            conversation,
            recorder: Mutex::new(RequestRecorder::for_transport(transport)),
        }
    }

    pub(crate) async fn record_request(
        &self,
        database: &DatabaseHandle,
        request_number: RequestNumber,
        body: &serde_json::Value,
        conversation: Option<&ChatConversation>,
        observed_at: Timestamp,
        cancellation: &CancellationToken,
    ) -> Result<(), DatabaseError> {
        database
            .record_development_request(
                &self.handoff,
                body,
                cancellation,
                || {
                    let decision = self.handoff.generative_inputs().is_none();
                    let mut pending_state = None;
                    let mut sent = if decision {
                        let body = entities::MessageBody::new_exact(
                            &serde_json::json!({"decision": body}),
                        )
                        .map_err(|_| {
                            DatabaseError::Rejected(Violation::InvalidValue(
                                "request",
                                "prepared decision cannot be recorded",
                            ))
                        })?;
                        let digest = body.digest();
                        entities::SentRequest {
                            job: self.handoff.job(),
                            request: request_number,
                            observed_at,
                            messages: vec![digest],
                            tools: None,
                            parameters: digest,
                            dropped_turns: None,
                            bodies: vec![body],
                            required_messages: Vec::new(),
                        }
                    } else {
                        let recorder = self
                            .recorder
                            .lock()
                            .unwrap_or_else(|poisoned| poisoned.into_inner());
                        let conversation = conversation.ok_or(DatabaseError::Rejected(
                            Violation::InvalidValue("request", "prepared conversation missing"),
                        ))?;
                        let (sent, state) = recorder
                            .prepare(self.handoff.job(), request_number, body, conversation, 0)
                            .map_err(|_| {
                                DatabaseError::Rejected(Violation::InvalidValue(
                                    "request",
                                    "prepared request cannot be recorded",
                                ))
                            })?;
                        pending_state = Some(state);
                        sent
                    };
                    sent.observed_at = observed_at;
                    Ok((sent, pending_state))
                },
                |state| {
                    if let Some(state) = state {
                        self.recorder
                            .lock()
                            .unwrap_or_else(|poisoned| poisoned.into_inner())
                            .remember_sent(state);
                    }
                },
            )
            .await
    }

    pub(crate) async fn record_answer(
        &self,
        database: &DatabaseHandle,
        answer: ProviderAnswer,
        usages: Vec<(u32, Metrics)>,
    ) -> Result<(), DatabaseError> {
        database
            .record_development_provider_answer(&self.handoff, answer, usages, |digest| {
                self.recorder
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner())
                    .remember_response(digest);
            })
            .await
    }
}

#[derive(Clone, Debug)]
pub(crate) struct DevelopmentToolOffer {
    pub(crate) wires: Vec<crate::ai::providers::openai_compatible::ToolWire>,
    pub(crate) grant: Option<&'static str>,
    pub(crate) agents_md: Option<String>,
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
) -> Result<PreparedDevelopmentExecution, DatabaseError> {
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
            let entities::MutableJobContent::Decision { request, .. } = &context.job.input.content
            else {
                return Err(unavailable());
            };
            let (request, conversation) =
                crate::ai::decisive_model::prepare_request(&context.configuration, request)
                    .map_err(|_| unavailable())?;
            Ok(PreparedDevelopmentExecution::new(
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
            let responses =
                frozen.llm.provider.transport == entities::LlmTransport::OpenAiResponses;
            let is_summary = snapshot.is_summary_work();
            if is_summary {
                let prompt = "You are a context summarizer. Return ONLY valid JSON matching {\"source\":\"...\",\"summary\":\"...\",\"omitted_details\":[...]}. Do not use tools, Markdown fences, or extra prose.";
                let mut messages = vec![if responses {
                    ChatMessage::developer(prompt)
                } else {
                    ChatMessage::system(prompt)
                }];
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
                let mut request =
                    crate::ai::generative_model::request_body(&frozen.llm, &conversation)
                        .map_err(|_| unavailable())?;
                if responses {
                    request["text"] = serde_json::json!({"format": {"type": "json_object"}});
                }
                return Ok(PreparedDevelopmentExecution::new(
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
                prior.push((
                    Some(format!(
                        "{}{}",
                        super::compact::ADOPTED_SUMMARY_NOTICE,
                        summary.summary.context_text()
                    )),
                    Vec::new(),
                    Vec::new(),
                ));
            }
            for input in &snapshot.covered_inputs {
                prior.push((
                    Some(input.text.clone()),
                    images_for(&input.attachments)?,
                    Vec::new(),
                ));
            }
            for turn in &snapshot.history {
                let mut replay = Vec::new();
                for message in &turn.response_replay {
                    if frozen.llm.provider.transport == entities::LlmTransport::AnthropicMessages {
                        super::providers::anthropic::replay(message, &mut replay)
                            .map_err(|_| unavailable())?;
                    } else if let Some(items) = message["output"]
                        .as_array()
                        .filter(|items| !items.is_empty())
                    {
                        replay.push(ChatMessage::responses(items.clone()));
                    } else if message["type"] == "function_call_output"
                        && let (Some(id), Some(output)) =
                            (message["call_id"].as_str(), message["output"].as_str())
                    {
                        replay.push(ChatMessage::tool(id, output));
                    } else {
                        return Err(unavailable());
                    }
                }
                let mut answer = if turn
                    .record
                    .outcome
                    .as_ref()
                    .is_some_and(|outcome| outcome.kind == entities::OutcomeKind::Cancelled)
                    || turn.record.job.phase != entities::MutableJobPhase::Executing
                    || (!replay.is_empty() && turn.awaiting_batch.is_empty())
                {
                    None
                } else {
                    turn.answer_text
                        .clone()
                        .or_else(|| {
                            turn.outputs
                                .last()
                                .and_then(entities::JobOutputRecord::display_text)
                        })
                        .filter(|text| !text.is_empty())
                };
                if !turn.awaiting_batch.is_empty() {
                    let context = paused_tool_context(&turn.awaiting_batch);
                    let answer = answer.get_or_insert_with(String::new);
                    if !answer.is_empty() {
                        answer.push_str("\n\n");
                    }
                    answer.push_str(&context);
                }
                if let Some(answer) = answer {
                    replay.push(ChatMessage::assistant(Some(answer), Vec::new()));
                }
                prior.push((
                    Some(turn.record.job.input.text().to_owned()),
                    images_for(&turn.attachments)?,
                    replay,
                ));
            }
            let current_images = images_for(&snapshot.attachments)?;
            if image_cursor.next().is_some() {
                return Err(unavailable());
            }
            let limits = ContextLimits::for_profile(&frozen.llm);
            let assignment = snapshot.job.input.text();
            let assignment_with_results = snapshot
                .prerequisite_results
                .as_ref()
                .map(|results| format!("{assignment}\n\n{results}"));
            let mut conversation = fit_with_notices(
                frozen,
                prior,
                Some(assignment_with_results.as_deref().unwrap_or(assignment)),
                &current_images,
                snapshot.project.as_ref(),
                offer.and_then(|offer| offer.grant),
                offer.and_then(|offer| offer.agents_md.as_deref()),
                offer.map(|offer| offer.wires.clone()).unwrap_or_default(),
                limits,
                ContextNotices::default(),
            )
            .ok_or_else(unavailable)?;
            if frozen.llm.provider.transport == entities::LlmTransport::OpenAiChatCompletions
                && offer.is_none_or(|offer| offer.grant.is_none())
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
            let request = crate::ai::generative_model::request_body(&frozen.llm, &conversation)
                .map_err(|_| unavailable())?;
            let mut verified = context.images.iter();
            for (index, message) in conversation.messages.iter().enumerate() {
                let mut ids = Vec::new();
                if let Some(MessageContentWire::Parts(parts)) = &message.content {
                    for part in parts {
                        let ContentPartWire::ImageUrl { image_url } = part else {
                            continue;
                        };
                        let (id, image) = verified.next().ok_or_else(unavailable)?;
                        if !matches!(image.part(), ContentPartWire::ImageUrl { image_url: expected } if expected == *image_url)
                        {
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
            Ok(PreparedDevelopmentExecution::new(
                DevelopmentCandidate::Generative(context.snapshot),
                context.images.into_iter().map(|(id, _)| id).collect(),
                request,
                Some(conversation),
                offer.is_some(),
            ))
        }
    }
}

fn paused_tool_context(batch: &[entities::ToolEvidenceRecord]) -> String {
    use entities::{ToolDecision, ToolOutcome};
    use serde_json::json;

    // Render prior calls as transcript text so a later Turn sees their recorded state.
    let calls = batch.iter().map(|call| {
        let data = match (&call.decision.fact, call.outcome.as_ref().map(|outcome| &outcome.fact)) {
            (ToolDecision::AwaitingApproval(_), _) => json!({"state": "awaited_approval"}),
            (ToolDecision::AwaitingAnswer(_), _) => json!({"state": "awaited_answer"}),
            (ToolDecision::Refused(reason), _) => json!({"state": "refused", "reason": reason.as_str()}),
            (ToolDecision::Admitted(_), Some(outcome)) => match outcome {
                ToolOutcome::Completed { result } => json!({"state": "completed", "result": result.as_str()}),
                ToolOutcome::NotApplied { message } => json!({"state": "not_applied", "reason": message}),
                ToolOutcome::NotInvoked { .. } => json!({"state": "not_invoked"}),
                ToolOutcome::CompletedElided { .. } | ToolOutcome::CompletedWithheld { .. } => {
                    json!({"state": "completed", "result_unavailable": true})
                }
                ToolOutcome::Uncertain { .. } => json!({"state": "uncertain"}),
            },
            (ToolDecision::Admitted(_), None) => json!({"state": "outcome_unavailable"}),
        };
        json!({"call_id": call.tool_call_id, "tool": call.name, "request": call.call.request.get(), "index": call.call.index, "evidence": data})
    }).collect::<Vec<_>>();
    format!(
        "Recorded tool calls from the previous Turn. Use completed results as history. Calls that awaited a decision did not run here. Follow only decisions supplied by the user.\n{}",
        json!(calls)
    )
}
