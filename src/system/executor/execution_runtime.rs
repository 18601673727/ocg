use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use super::tool_loop::{
    CallAnswers, CallSite, REQUEST_COMMAND_TOOL_NAME, ToolExecution, ToolLoopStop, answer_calls,
};
use crate::db::entities::{
    AppendedOutput, AvailabilityStat, FinalResult, IntermediateOutputKind, JobFinalization,
    JobOutput, Measurements, MessageBody, Metrics, RequestNumber, Timestamp, TokenAnnotation,
    Violation,
};
use crate::db::{DatabaseError, DatabaseHandle};
use crate::service::ai::compact::{ContextLimits, estimate_request_tokens};
use crate::service::ai::providers::openai_compatible::{ChatMessage, ChatToolCallWire};
use crate::service::ai::providers::{
    self, CancellationToken, OutputSinkError, ProviderExecutionConfig, ProviderExecutionError,
    ProviderExecutionResult, ProviderFact, ProviderObservation, ProviderOutputSink,
};

/// Evidence that this module observed the supervised in-process operation
/// return. This is not an OS-level stop proof and cannot be constructed by a
/// database or API caller. It is consumed by one finalization call.
pub(crate) struct DevelopmentExecutionReturn<'a> {
    handoff: &'a crate::db::DevelopmentHandoff,
}

impl<'a> DevelopmentExecutionReturn<'a> {
    pub(crate) fn handoff(&self) -> &'a crate::db::DevelopmentHandoff {
        self.handoff
    }
}

/// Which endpoints a development execution may send to.
#[derive(Clone, Copy)]
pub(crate) enum EndpointPolicy<'a> {
    /// A literal loopback endpoint only.
    Loopback,
    /// One manually approved external endpoint only, under the approval's
    /// own authority and control (the external trial or ordinary manual
    /// external execution); see [`crate::system::external_approval`].
    ApprovedExternal(&'a crate::system::external_approval::ExternalApproval),
    /// The one endpoint a durable scheduled external policy names for this
    /// Job's scheduled exchange, under
    /// [`crate::system::execution_controls::ExecutionControl::ScheduledExternalTransport`].
    /// It is never formed from a manual approval.
    ScheduledExternal(&'a crate::system::scheduled_external::ScheduledExternalGrant),
}

impl EndpointPolicy<'_> {
    fn permits(self, endpoint: &str, config: &ProviderExecutionConfig) -> bool {
        match self {
            EndpointPolicy::Loopback => crate::system::http::direct_diagnostic_loopback(endpoint),
            EndpointPolicy::ApprovedExternal(approval) => {
                approval.permits_endpoint(endpoint) && config.destination == approval.destination()
            }
            EndpointPolicy::ScheduledExternal(grant) => {
                grant.permits_endpoint(endpoint) && config.destination == grant.destination()
            }
        }
    }

    fn refusal(self) -> &'static str {
        match self {
            EndpointPolicy::Loopback => "direct loopback endpoint required",
            EndpointPolicy::ApprovedExternal(_) => "the approved external endpoint is required",
            EndpointPolicy::ScheduledExternal(_) => {
                "the durable scheduled external endpoint is required"
            }
        }
    }
}

/// The Generative exchange of a checked handoff, to the endpoints `policy`
/// permits: one request, or, when the Job's Agent was offered `tools`, the
/// requests of its tool loop. Every request is recorded as sent before it
/// leaves, and only after the previous one was answered.
pub(crate) async fn supervise_development_generative(
    database: &DatabaseHandle,
    handoff: crate::db::DevelopmentHandoff,
    config: &ProviderExecutionConfig,
    policy: EndpointPolicy<'_>,
    tools: Option<ToolExecution>,
) -> Result<AppendedOutput, DatabaseError> {
    let (frozen, first_conversation, prepared, token) =
        handoff
            .generative_inputs()
            .ok_or(DatabaseError::Rejected(Violation::InvalidValue(
                "handoff",
                "generative handoff required",
            )))?;
    let local_rejection =
        if config.max_output_bytes == 0 || config.max_output_bytes > 16 * 1024 * 1024 {
            Some("loopback output limit is invalid")
        } else {
            match frozen.llm.provider.endpoint.as_deref() {
                None => Some("explicit endpoint required"),
                Some(endpoint) if !policy.permits(endpoint, config) => Some(policy.refusal()),
                Some(_) => offer_mismatch(first_conversation, tools.as_ref()),
            }
        };
    if let Some(message) = local_rejection {
        return settle_development_unsent(
            database,
            &handoff,
            RequestNumber::FIRST,
            1,
            config,
            NOT_SENT,
            message,
        )
        .await;
    }
    let limits = ContextLimits::for_profile(&frozen.llm.config);
    let max_requests = tools
        .as_ref()
        .map_or(1, |tools| tools.limits.max_provider_requests);
    let mut conversation = first_conversation.clone();
    let mut body = prepared.clone();
    let mut request = RequestNumber::FIRST;
    let mut output_seq = 1u32;
    loop {
        let mut sink = DevelopmentLoopbackSink {
            database,
            handoff: &handoff,
            request,
            prepared: &body,
            cancellation: &config.cancellation,
            output_seq,
            sent: false,
            observations: Vec::new(),
            observation_overflow: false,
            streamed: String::new(),
        };
        let result = crate::service::ai::generative_model::execute_conversation(
            frozen,
            &conversation,
            &handoff,
            config,
            Some(&mut sink),
        )
        .await;
        output_seq = sink.output_seq;
        let observations = std::mem::take(&mut sink.observations);
        let (sent, overflow) = (sink.sent, sink.observation_overflow);
        let result = match result {
            Ok(result) if tools.is_none() => result.admitted(),
            other => other,
        };
        let result = match result {
            Ok(result) if tools.is_some() || !result.requests_tools() => result,
            Ok(_) => {
                return Err(DatabaseError::Rejected(Violation::InvalidValue(
                    "execution",
                    "the response requested tools, which this execution does not run; ownership remains held",
                )));
            }
            Err(error) => {
                return settle_development_error(
                    database, &handoff, request, output_seq, config, sent, error,
                )
                .await;
            }
        };
        if !sent || overflow {
            return Err(DatabaseError::Rejected(Violation::InvalidValue(
                "execution",
                "incomplete execution evidence; ownership remains held",
            )));
        }
        record_answered_request(
            database,
            &handoff,
            token.job,
            request,
            &result,
            &observations,
        )
        .await?;
        let Some(tools) = tools.as_ref().filter(|_| result.requests_tools()) else {
            let mut value = providers::result_value(&result);
            #[cfg(unix)]
            if let Some(workspace) = tools.as_ref().and_then(|tools| tools.workspace.as_ref()) {
                attach_file_snapshots(&mut value, workspace, &result.text).await;
            }
            let finalization = JobFinalization::Completed(FinalResult {
                summary: Some(result.text.clone()),
                code: None,
                goal_reached: None,
                value: Some(value),
            });
            let returned = DevelopmentExecutionReturn { handoff: &handoff };
            return database
                .finalize_development_job(returned, output_seq, finalization)
                .await;
        };
        // The calls are answered, and the conversation grows by the response
        // and its answers, before the next request is built.
        let site = CallSite {
            database,
            handoff: &handoff,
            request,
        };
        let answers = match answer_calls(
            tools,
            &site,
            &conversation,
            &result.tool_calls,
            &result.refused,
            &config.cancellation,
        )
        .await
        {
            Ok(CallAnswers::Answered(answers)) => answers,
            // The user's decision creates the Job that continues; this one
            // completes with what it waits for as its answer.
            Ok(CallAnswers::Awaiting(awaiting)) => {
                let finalization = JobFinalization::Completed(FinalResult {
                    summary: Some(awaiting_summary(&result, &awaiting)),
                    code: None,
                    goal_reached: None,
                    value: Some(providers::result_value(&result)),
                });
                let returned = DevelopmentExecutionReturn { handoff: &handoff };
                return database
                    .finalize_development_job(returned, output_seq, finalization)
                    .await;
            }
            Err(stop) => {
                return settle_tool_stop(database, &handoff, output_seq, Some(&result), stop).await;
            }
        };
        conversation.messages.push(assistant_message(&result));
        for (call, content) in result.tool_calls.iter().zip(answers) {
            conversation
                .messages
                .push(ChatMessage::tool(call.id.clone(), content));
        }
        let next = request
            .next()
            .filter(|next| next.get() <= max_requests)
            .ok_or(ToolLoopStop::RequestLimit {
                limit: max_requests,
            });
        let next = match next {
            Ok(next) => next,
            Err(stop) => {
                return settle_tool_stop(database, &handoff, output_seq, Some(&result), stop).await;
            }
        };
        if let Some(window) = limits.context_window
            && estimate_request_tokens(&conversation, limits.max_tokens) > u64::from(window)
        {
            return settle_tool_stop(
                database,
                &handoff,
                output_seq,
                Some(&result),
                ToolLoopStop::ContextWindow,
            )
            .await;
        }
        body = match crate::service::ai::generative_model::request_body(
            &frozen.llm.config.model,
            &frozen.llm.config.parameters,
            &conversation,
        ) {
            Ok(body) => body,
            Err(error) => {
                let stop = ToolLoopStop::ResponseUnserializable {
                    message: error.to_string(),
                };
                return settle_tool_stop(database, &handoff, output_seq, Some(&result), stop).await;
            }
        };
        request = next;
    }
}

#[cfg(unix)]
async fn capture_file_snapshots(
    workspace: super::workspace_read::Workspace,
    text: String,
) -> Vec<serde_json::Value> {
    use std::sync::atomic::{AtomicUsize, Ordering};

    static ACTIVE: AtomicUsize = AtomicUsize::new(0);
    static LINKS: std::sync::LazyLock<Result<regex::Regex, regex::Error>> =
        std::sync::LazyLock::new(|| regex::Regex::new(r"\]\(\s*<?([^\s)>]+)"));
    static LINES: std::sync::LazyLock<Result<regex::Regex, regex::Error>> =
        std::sync::LazyLock::new(|| regex::Regex::new(r"(?::(\d+)(?:-\d+)?|#L(\d+)(?:-L?\d+)?)$"));
    if ACTIVE
        .try_update(Ordering::AcqRel, Ordering::Acquire, |count| {
            (count < 4).then_some(count + 1)
        })
        .is_err()
    {
        return Vec::new();
    }
    struct Slot;
    impl Drop for Slot {
        fn drop(&mut self) {
            ACTIVE.fetch_sub(1, Ordering::AcqRel);
        }
    }
    let slot = Slot;
    let work = super::blocking::run(super::blocking::Kind::File, move || {
        let _slot = slot;
        let Ok(links) = LINKS.as_ref() else {
            return Vec::new();
        };
        let Ok(lines) = LINES.as_ref() else {
            return Vec::new();
        };
        let stamp = super::workspace_read::mutation_stamp();
        let mut seen = std::collections::HashSet::new();
        let mut snapshots = Vec::new();
        for link in links.captures_iter(&text).take(32) {
            let Some(target) = link.get(1).map(|value| value.as_str()) else {
                continue;
            };
            if target.contains("://") || target.starts_with('#') || target.starts_with("//") {
                continue;
            }
            let (path, line) = if let Some(location) = lines.captures(target) {
                let Some(matched) = location.get(0) else {
                    continue;
                };
                let line = location
                    .get(1)
                    .or_else(|| location.get(2))
                    .and_then(|value| value.as_str().parse::<u64>().ok())
                    .unwrap_or(1);
                (&target[..matched.start()], line.max(1))
            } else {
                (target, 1)
            };
            let reference_path = path.to_owned();
            let path = std::path::Path::new(path);
            let path = if path.is_absolute() {
                let Ok(relative) = path.strip_prefix(workspace.root()) else {
                    continue;
                };
                relative
            } else {
                path
            };
            let Some(path) = path.to_str() else { continue };
            let path = path.strip_prefix("./").unwrap_or(path);
            if !seen.insert(path.to_owned()) {
                continue;
            }
            if let Ok(mut snapshot) = workspace.snapshot_range(path, line) {
                let unchanged = snapshot
                    .get("content")
                    .and_then(|value| value.as_str())
                    .is_some_and(|content| {
                        matches!(
                            crate::db::entities::output_filter::text(content),
                            std::borrow::Cow::Borrowed(_)
                        )
                    });
                if !unchanged {
                    continue;
                }
                if let Some(object) = snapshot.as_object_mut() {
                    object.insert("path".into(), serde_json::Value::String(reference_path));
                }
                snapshots.push(snapshot);
                if snapshots.len() == 4 {
                    break;
                }
            }
        }
        if stamp.is_dirty() {
            Vec::new()
        } else {
            snapshots
        }
    });
    match ntex::util::select(work, ntex::time::sleep(std::time::Duration::from_secs(1))).await {
        ntex::util::Either::Left(Ok(snapshots)) => snapshots,
        ntex::util::Either::Left(Err(_)) | ntex::util::Either::Right(_) => Vec::new(),
    }
}

/// Adds the snapshots of the Workspace files an answer links to, to its result
/// value, when they are small and unchanged by the output filter.
#[cfg(unix)]
async fn attach_file_snapshots(
    value: &mut serde_json::Value,
    workspace: &super::workspace_read::Workspace,
    text: &str,
) {
    if text.len() > 256 * 1024 || !text.contains("](") {
        return;
    }
    let snapshots = capture_file_snapshots(workspace.clone(), text.to_owned()).await;
    if snapshots.is_empty() {
        return;
    }
    let Some(object) = value.as_object_mut() else {
        return;
    };
    object.insert("file_snapshots".into(), serde_json::Value::Array(snapshots));
    if crate::db::serialized_len(&*value).map_or(true, |bytes| bytes > 512 * 1024)
        && let Some(object) = value.as_object_mut()
    {
        object.remove("file_snapshots");
    }
}

/// What a Job that ends on calls waiting for the user says it waits for: the
/// question of each `ask_user` call and the request of each command request,
/// as its answer.
fn awaiting_summary(result: &ProviderExecutionResult, awaiting: &[u32]) -> String {
    let mut summary = String::new();
    for call in result
        .tool_calls
        .iter()
        .filter(|call| awaiting.contains(&call.index))
    {
        if !summary.is_empty() {
            summary.push('\n');
        }
        let arguments = serde_json::from_str::<serde_json::Value>(&call.arguments).ok();
        let text = |key: &str| {
            arguments
                .as_ref()
                .and_then(|value| value.get(key)?.as_str().map(str::to_owned))
                .unwrap_or_default()
        };
        if call.name == REQUEST_COMMAND_TOOL_NAME {
            let argv = arguments
                .as_ref()
                .and_then(|value| value.get("argv")?.as_array().cloned())
                .unwrap_or_default()
                .iter()
                .filter_map(|argument| argument.as_str().map(str::to_owned))
                .collect::<Vec<_>>()
                .join(" ");
            summary.push_str(&format!(
                "I ask your approval to add the command `{}` ({argv}) to this Project. {}",
                text("name"),
                text("reason"),
            ));
        } else {
            summary.push_str(&text("question"));
        }
    }
    summary
}

/// Why the tools a handoff was prepared with are not the tools this
/// execution holds, or `None` when they are the same.
fn offer_mismatch(
    conversation: &crate::service::ai::providers::openai_compatible::ChatConversation,
    tools: Option<&ToolExecution>,
) -> Option<&'static str> {
    match tools {
        None if conversation.tools.is_empty() => None,
        None => Some("the prepared request offers tools this execution does not hold"),
        Some(tools) => match tools.offer.wires() {
            Ok(wires)
                if serde_json::to_value(&wires).ok()
                    == serde_json::to_value(&conversation.tools).ok() =>
            {
                None
            }
            Ok(_) | Err(_) => Some("the tools changed since the request was prepared"),
        },
    }
}

/// Records the answer of one request and its usage. Only a response the
/// Provider completed reaches here, whether it asks for tools or answers.
async fn record_answered_request(
    database: &DatabaseHandle,
    handoff: &crate::db::DevelopmentHandoff,
    job: crate::db::entities::JobId,
    request: RequestNumber,
    result: &ProviderExecutionResult,
    observations: &[ProviderObservation],
) -> Result<(), DatabaseError> {
    let message = assistant_message(result);
    let response = MessageBody::new(&serde_json::to_value(message).map_err(|_| {
        DatabaseError::Rejected(Violation::InvalidValue(
            "response",
            "response cannot be serialized",
        ))
    })?)
    .map_err(|_| {
        DatabaseError::Rejected(Violation::InvalidValue(
            "response",
            "response cannot be recorded",
        ))
    })?;
    database
        .record_development_provider_answer(
            handoff,
            crate::db::entities::ProviderAnswer {
                stat: AvailabilityStat {
                    job,
                    request,
                    observed_at: Timestamp::now(),
                    observation: providers::success_observation(result),
                },
                measurements: providers::metrics(result),
                response,
            },
        )
        .await?;
    let mut usage_seq = 5u32;
    for observation in observations {
        if let ProviderFact::Usage { usage, finality } = &observation.fact {
            if [
                usage.input_tokens,
                usage.output_tokens,
                usage.cache_read_tokens,
                usage.cache_write_tokens,
                usage.reasoning_tokens,
            ]
            .iter()
            .all(Option::is_none)
            {
                continue;
            }
            database
                .record_development_usage(
                    handoff,
                    usage_seq,
                    Metrics {
                        job,
                        request,
                        observed_at: observation.observed_at,
                        measurements: Measurements {
                            usage: usage.clone(),
                            ..Measurements::default()
                        },
                        token: Some(TokenAnnotation::eligible(*finality)),
                    },
                )
                .await?;
            usage_seq = usage_seq
                .checked_add(2)
                .ok_or(DatabaseError::Rejected(Violation::ValueOutOfRange("usage")))?;
        }
    }
    Ok(())
}

/// Settles a Job whose tool loop stopped on this side. A known stop fails the
/// Job, a local orchestration failure that keeps the Provider's last
/// response, if there was one, as its value. A stop that leaves a call's
/// effect uncertain keeps the Allocation held as Uncertain. No Provider
/// failure is recorded for either.
async fn settle_tool_stop(
    database: &DatabaseHandle,
    handoff: &crate::db::DevelopmentHandoff,
    expected_seq: u32,
    last: Option<&ProviderExecutionResult>,
    stop: ToolLoopStop,
) -> Result<AppendedOutput, DatabaseError> {
    if stop.is_unknown() {
        database
            .record_development_uncertain(
                DevelopmentExecutionReturn { handoff },
                crate::db::entities::UncertaintyReason::StopRequestedWithoutConfirmedTermination,
            )
            .await?;
        return Err(DatabaseError::Rejected(Violation::InvalidValue(
            "execution",
            "the tool loop stopped with a call's effect unknown; ownership remains held as Uncertain",
        )));
    }
    let result = FinalResult {
        summary: Some(stop.to_string()),
        code: Some(TOOL_LOOP_STOPPED.to_owned()),
        goal_reached: Some(false),
        value: last.map(providers::result_value),
    };
    database
        .finalize_development_failure(
            DevelopmentExecutionReturn { handoff },
            expected_seq,
            JobFinalization::Failed(result),
            None,
        )
        .await
}

/// The native Decision protocol is distinct from Generative chat. A failed or
/// interrupted exchange retains ownership until a safe recovery protocol exists.
pub(crate) async fn supervise_development_decision(
    database: &DatabaseHandle,
    handoff: crate::db::DevelopmentHandoff,
    config: &ProviderExecutionConfig,
    policy: EndpointPolicy<'_>,
) -> Result<AppendedOutput, DatabaseError> {
    let (native, request, body, token) =
        handoff
            .decision_inputs()
            .ok_or(DatabaseError::Rejected(Violation::InvalidValue(
                "handoff",
                "Decision handoff required",
            )))?;
    let frozen = crate::db::entities::FrozenDecisionConfig::Native(native.clone());
    let local_rejection = match native.endpoint.as_deref() {
        None => Some((NOT_SENT, "explicit loopback endpoint required")),
        Some(endpoint) if !policy.permits(endpoint, config) => Some((NOT_SENT, policy.refusal())),
        Some(_) => {
            if crate::service::ai::decisive_model::request_body(&frozen, request)
                .ok()
                .as_ref()
                == Some(body)
            {
                None
            } else {
                Some((BUILD_FAILED, "prepared Decision request mismatch"))
            }
        }
    };
    if let Some((code, message)) = local_rejection {
        return settle_development_unsent(
            database,
            &handoff,
            RequestNumber::FIRST,
            1,
            config,
            code,
            message,
        )
        .await;
    }
    if config.cancellation.is_cancelled() {
        return settle_development_unsent(
            database,
            &handoff,
            RequestNumber::FIRST,
            1,
            config,
            NOT_SENT,
            "Execution was cancelled before the provider request.",
        )
        .await;
    }
    let mut sink = DevelopmentLoopbackSink {
        database,
        handoff: &handoff,
        request: RequestNumber::FIRST,
        prepared: body,
        cancellation: &config.cancellation,
        output_seq: 1,
        sent: false,
        observations: Vec::new(),
        observation_overflow: false,
        streamed: String::new(),
    };
    let result = crate::service::ai::decisive_model::execute_decision_with_sink(
        &frozen,
        request,
        body,
        &handoff,
        config,
        crate::db::entities::RequestRef {
            job: token.job,
            request: RequestNumber::FIRST,
        },
        &mut sink,
    )
    .await;
    let result = match result {
        Ok(result) => result,
        Err(error) => {
            return settle_development_error(
                database,
                &handoff,
                RequestNumber::FIRST,
                1,
                config,
                sink.sent,
                error,
            )
            .await;
        }
    };
    if !sink.sent {
        return Err(DatabaseError::Rejected(Violation::InvalidValue(
            "execution",
            "Decision request evidence missing; ownership remains held",
        )));
    }
    let response = MessageBody::new(&result.raw).map_err(|_| {
        DatabaseError::Rejected(Violation::InvalidValue(
            "response",
            "Decision response cannot be recorded",
        ))
    })?;
    database
        .record_development_provider_answer(
            &handoff,
            crate::db::entities::ProviderAnswer {
                stat: AvailabilityStat {
                    job: token.job,
                    request: RequestNumber::FIRST,
                    observed_at: Timestamp::now(),
                    observation: crate::db::entities::AvailabilityObservation {
                        status_code: Some(result.status_code),
                        finish_reason: None,
                        raw_finish_reason: None,
                        failure: None,
                    },
                },
                measurements: Measurements {
                    request_finished_in_ms: Some(
                        result.finished_in.as_millis().min(u128::from(u64::MAX)) as u64,
                    ),
                    ..Measurements::default()
                },
                response,
            },
        )
        .await?;
    let has_usage = [
        result.usage.input_tokens,
        result.usage.output_tokens,
        result.usage.cache_read_tokens,
        result.usage.cache_write_tokens,
        result.usage.reasoning_tokens,
    ]
    .iter()
    .any(Option::is_some);
    if has_usage {
        database
            .record_development_usage(
                &handoff,
                5,
                Metrics {
                    job: token.job,
                    request: RequestNumber::FIRST,
                    observed_at: Timestamp::now(),
                    measurements: Measurements {
                        usage: result.usage.clone(),
                        ..Measurements::default()
                    },
                    token: Some(TokenAnnotation::eligible(
                        crate::db::entities::UsageFinality::Final,
                    )),
                },
            )
            .await?;
    }
    let returned = DevelopmentExecutionReturn { handoff: &handoff };
    database
        .finalize_development_decision(returned, 1, result.result)
        .await
}

/// What a development exchange that produced no answer may be settled as.
/// Only facts this process observed qualify: a request that never left it, or
/// a Provider response that definitely refused the request. Anything else may
/// have escaped or may still be running remotely and keeps its ownership.
enum DevelopmentSettlement {
    Unsent {
        code: &'static str,
        message: String,
    },
    Refused {
        failure: Box<crate::service::ai::providers::openai_compatible::ProviderFailure>,
        observation: crate::db::entities::AvailabilityObservation,
    },
    /// Ownership stays held; the reason is what this process could not establish.
    Held(crate::db::entities::UncertaintyReason),
}

fn development_settlement(error: &ProviderExecutionError, sent: bool) -> DevelopmentSettlement {
    use crate::db::entities::{AvailabilityClassification, UncertaintyReason};
    match error {
        ProviderExecutionError::Build { .. } if !sent => DevelopmentSettlement::Unsent {
            code: BUILD_FAILED,
            message: error.to_string(),
        },
        ProviderExecutionError::NotDelivered { .. } if !sent => DevelopmentSettlement::Unsent {
            code: NOT_DELIVERED,
            message: error.to_string(),
        },
        ProviderExecutionError::Rejected { failure } if sent => {
            let observation = providers::error_observations(error).into_iter().next();
            match observation {
                Some(observation)
                    if matches!(
                        observation.classification(),
                        AvailabilityClassification::Rejected(_)
                    ) =>
                {
                    DevelopmentSettlement::Refused {
                        failure: Box::new(failure.clone()),
                        observation,
                    }
                }
                _ => DevelopmentSettlement::Held(UncertaintyReason::ResponseIncompleteOrInvalid),
            }
        }
        // Every other outcome is uncertain or incomplete: the request may
        // have been delivered, the response may be partial, or the status
        // does not prove a refusal. No retry, release or redispatch follows.
        other => DevelopmentSettlement::Held(uncertainty_reason(other)),
    }
}

/// The local reason an exchange error leaves the outcome uncertain. Each
/// reason states what this process could not establish, never that the
/// remote work ended.
fn uncertainty_reason(error: &ProviderExecutionError) -> crate::db::entities::UncertaintyReason {
    use crate::db::entities::UncertaintyReason;
    match error {
        // A Build or NotDelivered error reaches here only after the Sent
        // evidence was written, so delivery cannot be ruled out whatever the
        // transport reported.
        ProviderExecutionError::Build { .. }
        | ProviderExecutionError::NotDelivered { .. }
        | ProviderExecutionError::Unknown { facts: None, .. }
        | ProviderExecutionError::Interrupted { .. } => UncertaintyReason::RequestPossiblySent,
        ProviderExecutionError::Unknown { facts: Some(_), .. }
        | ProviderExecutionError::StreamFailure { .. } => UncertaintyReason::ConnectionInterrupted,
        ProviderExecutionError::Rejected { .. }
        | ProviderExecutionError::Incomplete { .. }
        | ProviderExecutionError::CompletionUnpersisted { .. } => {
            UncertaintyReason::ResponseIncompleteOrInvalid
        }
        ProviderExecutionError::Stopped { .. } => {
            UncertaintyReason::StopRequestedWithoutConfirmedTermination
        }
        ProviderExecutionError::OutputPersistence { stream, .. } => uncertainty_reason(stream),
    }
}

/// Settles a supervised exchange that returned an error. The in-process return
/// is observed here, so a local rejection is finalized; an uncertain remote
/// outcome is never turned into Failed or Cancelled.
async fn settle_development_error(
    database: &DatabaseHandle,
    handoff: &crate::db::DevelopmentHandoff,
    request: RequestNumber,
    expected_seq: u32,
    config: &ProviderExecutionConfig,
    sent: bool,
    error: ProviderExecutionError,
) -> Result<AppendedOutput, DatabaseError> {
    match development_settlement(&error, sent) {
        DevelopmentSettlement::Unsent { code, message } => {
            settle_development_unsent(
                database,
                handoff,
                request,
                expected_seq,
                config,
                code,
                &message,
            )
            .await
        }
        DevelopmentSettlement::Refused {
            failure,
            observation,
        } => {
            // A received refusal proves the exchange ended, not that no
            // provider-side work or charge occurred, so the text says so.
            let result = FinalResult {
                summary: Some(format!(
                    "{error}. A complete HTTP response was received; OCG cannot prove that the provider or an intermediary performed no work or incurred no charge."
                )),
                code: Some(PROVIDER_REJECTED.to_owned()),
                goal_reached: Some(false),
                value: Some(serde_json::json!({
                    "status_code": failure.status_code,
                    "provider_code": failure.provider_code,
                    "request_id": failure.request_id,
                    "retryable": failure.retryable,
                })),
            };
            database
                .finalize_development_failure(
                    DevelopmentExecutionReturn { handoff },
                    expected_seq,
                    JobFinalization::Failed(result),
                    Some((request, Timestamp::now(), observation)),
                )
                .await
        }
        DevelopmentSettlement::Held(reason) => {
            // The Allocation stays held. Recording the reason is what lets
            // an operator tell why; if the write fails the Allocation simply
            // stays Running, which is held all the same.
            database
                .record_development_uncertain(DevelopmentExecutionReturn { handoff }, reason)
                .await?;
            Err(DatabaseError::Rejected(Violation::InvalidValue(
                "execution",
                "provider exchange did not return a supported result; ownership remains held as Uncertain",
            )))
        }
    }
}

/// Settles an exchange whose request never left this process. Cancellation
/// before the request is Cancelled; every other local refusal is Failed. The
/// message is withheld if it would contain credential material.
async fn settle_development_unsent(
    database: &DatabaseHandle,
    handoff: &crate::db::DevelopmentHandoff,
    request: RequestNumber,
    expected_seq: u32,
    config: &ProviderExecutionConfig,
    code: &'static str,
    message: &str,
) -> Result<AppendedOutput, DatabaseError> {
    let cancelled = config.cancellation.is_cancelled();
    if cancelled && request != RequestNumber::FIRST {
        // A Job that already sent a request cannot end as Cancelled: what its
        // tools did is not known to have stopped, so it stays held.
        database
            .record_development_uncertain(
                DevelopmentExecutionReturn { handoff },
                crate::db::entities::UncertaintyReason::StopRequestedWithoutConfirmedTermination,
            )
            .await?;
        return Err(DatabaseError::Rejected(Violation::InvalidValue(
            "execution",
            "the tool loop was cancelled after a request; ownership remains held as Uncertain",
        )));
    }
    let summary = if handoff.contains_secret(message) {
        "Execution could not start the provider request.".to_owned()
    } else {
        message.to_owned()
    };
    let result = FinalResult {
        summary: Some(summary),
        code: Some(code.to_owned()),
        goal_reached: Some(false),
        value: None,
    };
    database
        .finalize_development_failure(
            DevelopmentExecutionReturn { handoff },
            expected_seq,
            if cancelled {
                JobFinalization::Cancelled(result)
            } else {
                JobFinalization::Failed(result)
            },
            None,
        )
        .await
}

struct DevelopmentLoopbackSink<'a> {
    database: &'a DatabaseHandle,
    handoff: &'a crate::db::DevelopmentHandoff,
    /// The request this exchange sends.
    request: RequestNumber,
    prepared: &'a serde_json::Value,
    /// The exchange's own cancellation. A stop that the database recorded
    /// before the request could be recorded as sent cancels it, so the
    /// exchange settles as cancelled before sending.
    cancellation: &'a CancellationToken,
    output_seq: u32,
    sent: bool,
    observations: Vec<ProviderObservation>,
    observation_overflow: bool,
    streamed: String,
}

impl ProviderOutputSink for DevelopmentLoopbackSink<'_> {
    fn sending<'a>(
        &'a mut self,
        body: &'a serde_json::Value,
    ) -> Pin<Box<dyn Future<Output = Result<(), OutputSinkError>> + 'a>> {
        Box::pin(async move {
            if self.sent || body != self.prepared {
                return Err(OutputSinkError {
                    message: "prepared request mismatch".into(),
                });
            }
            match self
                .database
                .record_development_request(self.handoff, self.request, body, Timestamp::now())
                .await
            {
                Ok(()) => {}
                Err(DatabaseError::Rejected(Violation::InvalidValue("execution", reason)))
                    if reason == crate::db::entities::STOPPED_BEFORE_SEND =>
                {
                    self.cancellation.cancel();
                    return Err(OutputSinkError {
                        message: "a stop was requested before the request was sent".into(),
                    });
                }
                Err(_) => {
                    return Err(OutputSinkError {
                        message: "request evidence unavailable".into(),
                    });
                }
            }
            self.sent = true;
            Ok(())
        })
    }
    fn append<'a>(
        &'a mut self,
        kind: IntermediateOutputKind,
        text: &'a str,
    ) -> Pin<Box<dyn Future<Output = Result<(), OutputSinkError>> + 'a>> {
        Box::pin(async move {
            self.streamed.push_str(text);
            if self.handoff.contains_secret(&self.streamed) {
                return Err(OutputSinkError {
                    message: "output contains credential material".into(),
                });
            }
            self.database
                .append_development_output(
                    self.handoff,
                    self.output_seq,
                    JobOutput {
                        kind,
                        summary: Some(text.to_owned()),
                        code: None,
                        goal_reached: None,
                        value: None,
                    },
                )
                .await
                .map_err(|_| OutputSinkError {
                    message: "output evidence unavailable".into(),
                })?;
            self.output_seq = self.output_seq.checked_add(1).ok_or(OutputSinkError {
                message: "output sequence exhausted".into(),
            })?;
            Ok(())
        })
    }
    fn observe<'a>(
        &'a mut self,
        observation: ProviderObservation,
    ) -> Pin<Box<dyn Future<Output = ()> + 'a>> {
        Box::pin(async move {
            if self.observations.len() >= 1024 {
                self.observation_overflow = true;
            } else {
                self.observations.push(observation);
            }
        })
    }
}

/// A controlled local fixture. No variant invokes a provider, tool, or child
/// execution. It is available only to trusted development-build components.
#[allow(dead_code)]
pub(crate) enum DevelopmentSyntheticExecution {
    Generative {
        observed_at: Timestamp,
        sent: bool,
        answer: Option<crate::db::entities::ProviderAnswer>,
        usage: Option<Metrics>,
        finalization: JobFinalization,
    },
    Decision {
        observed_at: Timestamp,
        answer: crate::db::entities::ProviderAnswer,
        result: crate::db::entities::DecisionResult,
    },
}

/// Exercise the native Worker path without dispatch or external effects. The
/// handoff is owned until the operation returns; a dropped future cannot issue
/// a return proof. A failed or uncertain evidence write leaves ownership held.
#[allow(dead_code)]
pub(crate) async fn supervise_development_synthetic(
    database: &DatabaseHandle,
    handoff: crate::db::DevelopmentHandoff,
    execution: DevelopmentSyntheticExecution,
) -> Result<AppendedOutput, DatabaseError> {
    enum Returned {
        Generative(JobFinalization),
        Decision(Box<crate::db::entities::DecisionResult>),
    }
    let result = async {
        match execution {
            DevelopmentSyntheticExecution::Generative {
                observed_at,
                sent,
                answer,
                usage,
                finalization,
            } => {
                if sent {
                    database
                        .record_development_sent_request(&handoff, observed_at)
                        .await?;
                }
                if let Some(answer) = answer {
                    database
                        .record_development_provider_answer(&handoff, answer)
                        .await?;
                }
                if let Some(usage) = usage {
                    database
                        .record_development_usage(&handoff, 5, usage)
                        .await?;
                }
                Ok::<_, DatabaseError>(Returned::Generative(finalization))
            }
            DevelopmentSyntheticExecution::Decision {
                observed_at,
                answer,
                result,
            } => {
                database
                    .record_development_sent_request(&handoff, observed_at)
                    .await?;
                database
                    .record_development_provider_answer(&handoff, answer)
                    .await?;
                Ok(Returned::Decision(Box::new(result)))
            }
        }
    }
    .await?;
    let returned = DevelopmentExecutionReturn { handoff: &handoff };
    match result {
        Returned::Generative(finalization) => {
            database
                .finalize_development_job(returned, 1, finalization)
                .await
        }
        Returned::Decision(result) => {
            database
                .finalize_development_decision(returned, 1, *result)
                .await
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub enum ExecutionRuntimeError {
    #[error("scheduler worker stopped")]
    SchedulerStopped,
    #[error("database operation failed: {0}")]
    Database(#[from] DatabaseError),
    #[error("execution worker panicked")]
    WorkerPanicked,
    #[error("the execution runtime is shutting down")]
    ShuttingDown,
}

/// What a Provider execution needs to turn a Job's Project into tools: the
/// data directory its root must stay apart from. Nothing of a Project is
/// kept here; it is read again for every Job, see [`resolve_project`].
#[derive(Clone, Debug)]
pub(crate) struct WorkspaceTools {
    data_directory: Arc<crate::system::data_directory::DataDirectory>,
}

impl WorkspaceTools {
    pub(crate) fn new(data_directory: crate::system::data_directory::DataDirectory) -> Self {
        Self {
            data_directory: Arc::new(data_directory),
        }
    }

    pub(crate) fn data_directory(&self) -> &crate::system::data_directory::DataDirectory {
        &self.data_directory
    }
}

const TOOL_LOOP_STOPPED: &str = "tool_loop_stopped";
const PROVIDER_REJECTED: &str = "provider_rejected";
const NOT_DELIVERED: &str = "not_delivered";
const BUILD_FAILED: &str = "build_failed";
const NOT_SENT: &str = "not_sent";
/// Records the facts of one answered request: its timing and its
/// availability observation. Returns the first write that failed.
fn assistant_message(result: &ProviderExecutionResult) -> ChatMessage {
    ChatMessage::assistant(
        Some(result.text.clone()).filter(|text| !text.is_empty()),
        result
            .tool_calls
            .iter()
            .map(ChatToolCallWire::from)
            .collect(),
    )
}
