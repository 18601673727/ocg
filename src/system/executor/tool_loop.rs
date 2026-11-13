use std::future::Future;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::Duration;

use ntex::util::{Either, select};

use super::tools::{FailureCertainty, ToolFailure, ToolFuture, ToolOffer, ToolStart};
use crate::db::entities::{
    AuthorizationBasis, ContentDigest, JobToolCall, MAX_STORED_TOOL_ARGUMENTS_BYTES,
    NotInvokedReason, RequestNumber, ToolAdmission, ToolCallRef, ToolDecision, ToolEffect,
    ToolEvidence, ToolOutcome, ToolOutcomeKind, ToolPayload, UncertainCause, Violation,
    WithheldReason,
};
use crate::db::{DatabaseError, DatabaseHandle, DevelopmentHandoff};
use crate::service::ai::providers::CancellationToken;
use crate::service::ai::providers::openai_compatible::{
    ChatConversation, CompletedToolCall, InvalidToolCall, ToolCallRefusal,
};

const DEFAULT_MAX_PROVIDER_REQUESTS: u32 = 2048;

const HANDLER_TIMEOUT: Duration = Duration::from_secs(120 + 5);

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct ToolLoopLimits {
    pub(crate) handler_timeout: Duration,
    pub(crate) max_provider_requests: u32,
}

impl Default for ToolLoopLimits {
    fn default() -> Self {
        Self {
            handler_timeout: HANDLER_TIMEOUT,
            max_provider_requests: DEFAULT_MAX_PROVIDER_REQUESTS,
        }
    }
}

#[derive(Clone, Debug)]
pub(crate) struct ToolExecution {
    pub(crate) decider: bool,
    pub(crate) offer: ToolOffer,
    pub(crate) limits: ToolLoopLimits,
    pub(crate) stops: crate::system::native_scheduler::NativeStops,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum ToolLoopStop {
    RequestLimit {
        limit: u32,
    },
    Refused(InvalidToolCall),
    AdmissionRefused {
        index: u32,
        violation: Violation,
    },
    HandlerUncertain {
        index: u32,
        message: String,
    },
    ResultUnserializable {
        index: u32,
    },
    ResponseUnserializable {
        message: String,
    },
    TimedOut {
        index: u32,
    },
    Cancelled,
    EvidenceNotRecorded {
        index: u32,
        outcome: Option<ToolOutcomeKind>,
    },
    ContextWindow,
}

impl ToolLoopStop {
    pub(crate) fn is_unknown(&self) -> bool {
        match self {
            Self::TimedOut { .. }
            | Self::Cancelled
            | Self::HandlerUncertain { .. }
            | Self::EvidenceNotRecorded { .. } => true,
            Self::RequestLimit { .. }
            | Self::Refused(_)
            | Self::AdmissionRefused { .. }
            | Self::ResultUnserializable { .. }
            | Self::ResponseUnserializable { .. }
            | Self::ContextWindow => false,
        }
    }
}

impl std::fmt::Display for ToolLoopStop {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Refused(refusal) => write!(formatter, "a tool call was refused: {refusal}"),
            Self::AdmissionRefused { index, violation } => write!(
                formatter,
                "the admission of tool call {index} was refused: {violation}"
            ),
            Self::HandlerUncertain { index, message } => write!(
                formatter,
                "tool call {index} failed and its effect is uncertain: {message}"
            ),
            Self::ResultUnserializable { index } => write!(
                formatter,
                "the result of tool call {index} cannot be serialized"
            ),
            Self::ResponseUnserializable { message } => write!(
                formatter,
                "the assistant response could not be recorded: {message}"
            ),
            Self::TimedOut { index } => write!(formatter, "tool call {index} timed out"),
            Self::Cancelled => formatter.write_str("the tool loop was cancelled"),
            Self::EvidenceNotRecorded { index, .. } => write!(
                formatter,
                "the evidence of tool call {index} could not be recorded"
            ),
            Self::RequestLimit { limit } => write!(
                formatter,
                "the Provider request budget of {limit} was exhausted"
            ),
            Self::ContextWindow => {
                formatter.write_str("the conversation exceeds the model's context window")
            }
        }
    }
}

pub(crate) const ASK_USER_TOOL_NAME: &str = "ask_user";

pub(crate) const REQUEST_COMMAND_TOOL_NAME: &str = "workspace_request_command";

fn needs_decision(name: &str) -> Option<fn(ToolAdmission) -> ToolDecision> {
    match name {
        ASK_USER_TOOL_NAME | "job_dispatch" => Some(ToolDecision::AwaitingAnswer),
        REQUEST_COMMAND_TOOL_NAME | "job_assign" => Some(ToolDecision::AwaitingApproval),
        _ => None,
    }
}

#[derive(Debug)]
pub(crate) enum CallAnswers {
    Answered(Vec<String>),
    Awaiting(Vec<u32>),
}

pub(crate) struct CallSite<'a> {
    pub(crate) database: &'a DatabaseHandle,
    pub(crate) handoff: &'a DevelopmentHandoff,
    pub(crate) request: RequestNumber,
}

impl CallSite<'_> {
    fn call(&self, call: &CompletedToolCall) -> ToolCallRef {
        ToolCallRef {
            job: self.handoff_job(),
            request: self.request,
            index: call.index,
        }
    }

    fn handoff_job(&self) -> crate::db::entities::JobId {
        self.handoff.job()
    }
}

struct CatchPanic(ToolFuture<'static>);

impl Future for CatchPanic {
    type Output = Result<serde_json::Value, ToolFailure>;

    fn poll(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        let call = &mut self.0;
        match catch_unwind(AssertUnwindSafe(|| call.as_mut().poll(context))) {
            Ok(polled) => polled,
            Err(_) => Poll::Ready(Err(ToolFailure::uncertain("the tool handler panicked"))),
        }
    }
}

fn refusal_answer(refusal: &InvalidToolCall) -> String {
    serde_json::json!({"refused": refusal.kind.as_str(), "reason": refusal.message}).to_string()
}

pub(crate) async fn answer_calls(
    tools: &ToolExecution,
    site: &CallSite<'_>,
    conversation: &ChatConversation,
    calls: &[CompletedToolCall],
    refused: &[InvalidToolCall],
    cancellation: &CancellationToken,
) -> Result<CallAnswers, ToolLoopStop> {
    if cancellation.is_cancelled() {
        return Err(ToolLoopStop::Cancelled);
    }
    let earlier = conversation
        .messages
        .iter()
        .filter_map(|message| message.tool_calls.as_deref())
        .flatten()
        .map(|call| call.id.as_str())
        .collect::<std::collections::HashSet<_>>();
    for call in calls {
        if earlier.contains(call.id.as_str()) {
            return Err(ToolLoopStop::Refused(InvalidToolCall::at(
                ToolCallRefusal::ConflictingIdentity,
                call.index,
                format!("tool call {} repeats the id of an earlier call", call.index),
            )));
        }
        if refused
            .iter()
            .any(|refusal| refusal.index == Some(call.index))
        {
            continue;
        }
        if let Err(refusal) = tools.offer.resolve(call) {
            return Err(refuse(site, call, refusal).await);
        }
        if call.arguments.len() > MAX_STORED_TOOL_ARGUMENTS_BYTES {
            let refusal = InvalidToolCall::at(
                ToolCallRefusal::LimitExceeded,
                call.index,
                format!(
                    "tool call {} has arguments beyond what OCG records",
                    call.index
                ),
            );
            return Err(refuse(site, call, refusal).await);
        }
    }

    let limits = &tools.limits;
    let mut answers = Vec::with_capacity(calls.len());
    let mut awaiting = Vec::new();
    for call in calls {
        if cancellation.is_cancelled() {
            return Err(ToolLoopStop::Cancelled);
        }
        if let Some(refusal) = refused
            .iter()
            .find(|refusal| refusal.index == Some(call.index))
        {
            record_refusal(site, call, refusal).await?;
            answers.push(refusal_answer(refusal));
            continue;
        }
        let dispatch = match tools.offer.resolve(call) {
            Ok(dispatch) => dispatch,
            Err(refusal) => return Err(refuse(site, call, refusal).await),
        };
        let index = call.index;
        let reference = site.call(call);
        let effect = dispatch.effect;
        let stoppable = dispatch.spawns;
        let decided = ToolAdmission {
            handler_version: dispatch.version.clone(),
            effect,
            parameters: dispatch.schema.clone(),
            arguments: ToolPayload::new(call.arguments.clone()),
            authorization: AuthorizationBasis::RegistryGrant,
        };
        if let Some(awaiting_decision) = needs_decision(&call.name) {
            if tools.decider {
                record_decision(site, call, awaiting_decision(decided)).await?;
                awaiting.push(index);
                answers.push(String::new());
            } else {
                record_decision(
                    site,
                    call,
                    ToolDecision::Refused(ToolCallRefusal::NoDecider),
                )
                .await?;
                answers.push(
                    serde_json::json!({"refused": "no_decider", "reason": "no user decides for this execution"})
                        .to_string(),
                );
            }
            continue;
        }
        let spawns = decided.spawns();
        let mut starting = spawns.then(|| super::blocking::begin_call(reference));
        if let Some(starting) = &mut starting {
            starting.admission_pending();
        }
        let admission = ToolEvidence {
            approval: None,
            call: reference,
            tool_call_id: call.id.clone(),
            name: call.name.clone(),
            spawns,
            decision: ToolDecision::Admitted(decided),
        };
        match site
            .database
            .record_development_tool_decision(site.handoff, admission)
            .await
        {
            Ok(()) => {}
            Err(DatabaseError::Rejected(violation)) => {
                if let Some(starting) = &mut starting {
                    starting.admission_refused();
                }
                return Err(ToolLoopStop::AdmissionRefused { index, violation });
            }
            Err(_) => {
                return Err(ToolLoopStop::EvidenceNotRecorded {
                    index,
                    outcome: None,
                });
            }
        }
        if cancellation.is_cancelled() {
            return Err(cancelled_before_invocation(site, reference).await);
        }
        let dispatch = match tools.offer.resolve(call) {
            Ok(dispatch) => dispatch,
            Err(refusal) => {
                let outcome = ToolOutcome::NotInvoked {
                    reason: NotInvokedReason::Refused(refusal.kind),
                };
                return Err(
                    conclude(site, reference, outcome, ToolLoopStop::Refused(refusal)).await,
                );
            }
        };
        if JobToolCall::is_job_tool(&call.name) {
            let Some(job_call) = JobToolCall::parse(&call.name, &call.arguments) else {
                let message = "the arguments do not fit the job tool".to_owned();
                answer_failed(
                    site,
                    reference,
                    index,
                    ToolOutcome::NotApplied {
                        message: message.clone(),
                    },
                    "not_applied",
                    &message,
                    &mut answers,
                )
                .await?;
                continue;
            };
            match site
                .database
                .apply_development_job_tool(site.handoff, reference, job_call)
                .await
            {
                Ok(applied) => {
                    for token in applied.running {
                        tools.stops.cancel(token);
                    }
                    if let Err(error) = site.database.settle_child_turns(applied.ended).await {
                        log::warn!(
                            "the parents of the turns a job tool ended were not told: {error}"
                        );
                    }
                    answers.push(applied.value.to_string());
                    continue;
                }
                Err(DatabaseError::Rejected(violation)) => {
                    let message = violation.to_string();
                    answer_failed(
                        site,
                        reference,
                        index,
                        ToolOutcome::NotApplied {
                            message: message.clone(),
                        },
                        "not_applied",
                        &message,
                        &mut answers,
                    )
                    .await?;
                    continue;
                }
                Err(_) => {
                    return Err(ToolLoopStop::EvidenceNotRecorded {
                        index,
                        outcome: None,
                    });
                }
            }
        }
        let handler = dispatch.handler;
        let arguments = dispatch.arguments;
        let stopping_handler = handler.clone();
        let start = ToolStart::with_starting(starting);
        let invocation_start = start.clone();
        let invocation = CatchPanic(Box::pin(async move {
            handler
                .call_with_start(reference, arguments, invocation_start)
                .await
        }));
        let timed = async {
            let expires = async {
                start.wait().await;
                ntex::time::sleep(start.remaining(limits.handler_timeout)).await;
            };
            match select(invocation, expires).await {
                Either::Left(result) => Ok(result),
                Either::Right(()) => Err(()),
            }
        };
        let ran = select(cancellation.cancelled(), timed).await;

        let stopped = match &ran {
            Either::Left(()) | Either::Right(Err(())) | Either::Right(Ok(Err(_))) if stoppable => {
                stopping_handler.stop(site.handoff_job()).await
            }
            _ => Ok(()),
        };
        if let Err(failure) = stopped {
            return Err(ToolLoopStop::HandlerUncertain {
                index,
                message: failure.message,
            });
        }
        let read_only = effect == ToolEffect::ReadOnly;
        let (outcome, stop) = match ran {
            Either::Left(()) if !start.has_started() => (
                ToolOutcome::NotApplied {
                    message: "cancelled before the handler started".into(),
                },
                ToolLoopStop::Cancelled,
            ),
            Either::Left(()) => (
                ToolOutcome::Uncertain {
                    cause: UncertainCause::Cancelled,
                    message: None,
                },
                ToolLoopStop::Cancelled,
            ),
            Either::Right(Err(())) => {
                let outcome = ToolOutcome::Uncertain {
                    cause: UncertainCause::TimedOut,
                    message: None,
                };
                if read_only {
                    answer_failed(
                        site,
                        reference,
                        index,
                        outcome,
                        "timed_out",
                        "the call did not finish within its time limit",
                        &mut answers,
                    )
                    .await?;
                    continue;
                }
                (outcome, ToolLoopStop::TimedOut { index })
            }
            Either::Right(Ok(Err(failure))) => match failure.certainty {
                FailureCertainty::NotApplied => {
                    let message = failure.message;
                    let outcome = ToolOutcome::NotApplied {
                        message: message.clone(),
                    };
                    answer_failed(
                        site,
                        reference,
                        index,
                        outcome,
                        "not_applied",
                        &message,
                        &mut answers,
                    )
                    .await?;
                    continue;
                }
                FailureCertainty::Uncertain if read_only => {
                    let outcome = ToolOutcome::Uncertain {
                        cause: UncertainCause::HandlerReported,
                        message: Some(failure.message.clone()),
                    };
                    answer_failed(
                        site,
                        reference,
                        index,
                        outcome,
                        "failed",
                        &failure.message,
                        &mut answers,
                    )
                    .await?;
                    continue;
                }
                FailureCertainty::Uncertain => (
                    ToolOutcome::Uncertain {
                        cause: UncertainCause::HandlerReported,
                        message: Some(failure.message.clone()),
                    },
                    ToolLoopStop::HandlerUncertain {
                        index,
                        message: failure.message,
                    },
                ),
            },
            Either::Right(Ok(Ok(value))) => match serde_json::to_string(&value) {
                Err(_) => (
                    ToolOutcome::CompletedWithheld {
                        reason: WithheldReason::Unserializable,
                    },
                    ToolLoopStop::ResultUnserializable { index },
                ),
                Ok(content) => {
                    let outcome = if read_only {
                        ToolOutcome::CompletedElided {
                            bytes: content.len() as u64,
                            digest: ContentDigest::of(content.as_bytes()),
                        }
                    } else {
                        ToolOutcome::Completed {
                            result: ToolPayload::new(content.clone()),
                        }
                    };
                    let kind = outcome.kind();
                    if site
                        .database
                        .record_development_tool_outcome(site.handoff, reference, outcome)
                        .await
                        .is_err()
                    {
                        return Err(ToolLoopStop::EvidenceNotRecorded {
                            index,
                            outcome: Some(kind),
                        });
                    }
                    answers.push(content);
                    continue;
                }
            },
        };
        return Err(conclude(site, reference, outcome, stop).await);
    }
    Ok(if awaiting.is_empty() {
        CallAnswers::Answered(answers)
    } else {
        CallAnswers::Awaiting(awaiting)
    })
}

async fn record_decision(
    site: &CallSite<'_>,
    call: &CompletedToolCall,
    decision: ToolDecision,
) -> Result<(), ToolLoopStop> {
    let evidence = ToolEvidence {
        approval: None,
        call: site.call(call),
        tool_call_id: call.id.clone(),
        name: call.name.clone(),
        spawns: false,
        decision,
    };
    match site
        .database
        .record_development_tool_decision(site.handoff, evidence)
        .await
    {
        Ok(()) => Ok(()),
        Err(DatabaseError::Rejected(violation)) => Err(ToolLoopStop::AdmissionRefused {
            index: call.index,
            violation,
        }),
        Err(_) => Err(ToolLoopStop::EvidenceNotRecorded {
            index: call.index,
            outcome: None,
        }),
    }
}

async fn answer_failed(
    site: &CallSite<'_>,
    call: ToolCallRef,
    index: u32,
    outcome: ToolOutcome,
    failed: &str,
    reason: &str,
    answers: &mut Vec<String>,
) -> Result<(), ToolLoopStop> {
    let kind = outcome.kind();
    if site
        .database
        .record_development_tool_outcome(site.handoff, call, outcome)
        .await
        .is_err()
    {
        return Err(ToolLoopStop::EvidenceNotRecorded {
            index,
            outcome: Some(kind),
        });
    }
    answers.push(serde_json::json!({"failed": failed, "reason": reason}).to_string());
    Ok(())
}

async fn refuse(
    site: &CallSite<'_>,
    call: &CompletedToolCall,
    refusal: InvalidToolCall,
) -> ToolLoopStop {
    match record_refusal(site, call, &refusal).await {
        Ok(()) => ToolLoopStop::Refused(refusal),
        Err(stop) => stop,
    }
}

async fn record_refusal(
    site: &CallSite<'_>,
    call: &CompletedToolCall,
    refusal: &InvalidToolCall,
) -> Result<(), ToolLoopStop> {
    site.database
        .record_development_tool_decision(
            site.handoff,
            ToolEvidence {
                approval: None,
                call: site.call(call),
                tool_call_id: call.id.clone(),
                name: call.name.clone(),
                decision: ToolDecision::Refused(refusal.kind),
                spawns: false,
            },
        )
        .await
        .map_err(|_| ToolLoopStop::Refused(refusal.clone()))
}

async fn cancelled_before_invocation(site: &CallSite<'_>, call: ToolCallRef) -> ToolLoopStop {
    let outcome = ToolOutcome::NotInvoked {
        reason: NotInvokedReason::Cancelled,
    };
    conclude(site, call, outcome, ToolLoopStop::Cancelled).await
}

async fn conclude(
    site: &CallSite<'_>,
    call: ToolCallRef,
    outcome: ToolOutcome,
    stop: ToolLoopStop,
) -> ToolLoopStop {
    let kind = outcome.kind();
    match site
        .database
        .record_development_tool_outcome(site.handoff, call, outcome)
        .await
    {
        Ok(()) => stop,
        Err(_) if stop.is_unknown() => stop,
        Err(_) => ToolLoopStop::EvidenceNotRecorded {
            index: call.index,
            outcome: Some(kind),
        },
    }
}
