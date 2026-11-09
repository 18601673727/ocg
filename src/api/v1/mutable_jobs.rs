use super::contracts::envelope;
use super::*;
use crate::db::entities::{
    ComposerContextId, ExecutionConfiguration, JobQueuePriority, MutableJobContent,
    MutableJobInput, MutableJobPhase, MutableJobRecord, OutcomeKind, ReasoningEffort,
    RequestFingerprint, Submission, Violation,
};
use aws_lc_rs::digest::{Context, SHA256};
use serde::Deserialize;

const INVALID: Refusal = Refusal::new(
    StatusCode::BAD_REQUEST,
    "invalid_mutable_job",
    "invalid mutable Job request",
);
const CONFLICT: Refusal = Refusal::new(
    StatusCode::CONFLICT,
    "mutable_job_conflict",
    "the Job revision or lifecycle does not permit this change",
);
const UNSUPPORTED_CONTEXT: Refusal = Refusal::new(
    StatusCode::CONFLICT,
    "unsupported_execution_context",
    "the selected conversation context cannot be executed",
);
const UNAVAILABLE: Refusal = Refusal::new(
    StatusCode::SERVICE_UNAVAILABLE,
    "mutable_jobs_unavailable",
    "mutable Jobs are unavailable in this database",
);
const UNCERTAIN: Refusal = Refusal::new(
    StatusCode::SERVICE_UNAVAILABLE,
    "mutable_job_outcome_unknown",
    "the operation may have committed; reconcile the Job before retrying",
);

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Input {
    #[serde(default)]
    conversation: Option<String>,
    #[serde(default)]
    project: Option<String>,
    #[serde(default)]
    kind: Option<String>,
    #[serde(default)]
    text: Option<String>,
    #[serde(default)]
    attachments: Vec<String>,
    configuration: Option<Configuration>,
    #[serde(default)]
    decision_request: Option<crate::db::entities::DecisionRequest>,
    #[serde(default)]
    decision_profile: Option<String>,
    #[serde(default)]
    priority: Option<String>,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Configuration {
    llm_profile: String,
    agent_profile: Option<String>,
    reasoning_effort: Option<String>,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Change {
    revision: u64,
    input: Input,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Submit {
    revision: u64,
    input: Option<Input>,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct CancelQueued {
    revision: u64,
}

fn context(request: &HttpRequest) -> Result<ComposerContextId, Refusal> {
    let name = header::HeaderName::from_static("x-ocg-composer-context");
    let value = single(request.headers(), &name)
        .and_then(|v| v.to_str().ok())
        .ok_or(INVALID)?;
    crate::db::entities::parse_entity_id(value)
        .map(ComposerContextId::new)
        .ok_or(INVALID)
}
pub(super) fn optional_context(
    request: &HttpRequest,
) -> Result<Option<ComposerContextId>, Refusal> {
    let name = header::HeaderName::from_static("x-ocg-composer-context");
    if request.headers().contains_key(&name) {
        context(request).map(Some)
    } else {
        Ok(None)
    }
}
fn convert(input: Input) -> Result<MutableJobInput, Refusal> {
    let attachments = input
        .attachments
        .iter()
        .map(|id| attachments::attachment_id(id))
        .collect::<Result<Vec<_>, _>>()?;
    let priority = match input.priority.as_deref().unwrap_or("foreground") {
        "foreground" => JobQueuePriority::Foreground,
        "automatic" => JobQueuePriority::Automatic,
        "speculative" => JobQueuePriority::Speculative,
        _ => return Err(INVALID),
    };
    let configuration = input
        .configuration
        .map(|config| -> Result<_, Refusal> {
            Ok(ExecutionConfiguration {
                llm_profile: profiles::profile_id(&config.llm_profile)?,
                agent_profile: config
                    .agent_profile
                    .as_deref()
                    .map(profiles::profile_id)
                    .transpose()?,
                reasoning_effort: config
                    .reasoning_effort
                    .as_deref()
                    .map(|effort| ReasoningEffort::parse(effort).ok_or(INVALID))
                    .transpose()?,
            })
        })
        .transpose()?;
    let content = match input.kind.as_deref().unwrap_or("generative") {
        "generative" if input.decision_request.is_none() && input.decision_profile.is_none() => {
            MutableJobContent::Generative {
                text: input.text.ok_or(INVALID)?,
                attachments,
                configuration,
            }
        }
        "decision" if input.text.is_none() && attachments.is_empty() && configuration.is_none() => {
            MutableJobContent::Decision {
                request: input.decision_request.ok_or(INVALID)?,
                profile: profiles::profile_id(input.decision_profile.as_deref().ok_or(INVALID)?)?
                    .into(),
                references: Vec::new(),
            }
        }
        _ => return Err(INVALID),
    };
    Ok(MutableJobInput { content, priority })
}
fn submission(
    request: &HttpRequest,
    body: &[u8],
    domain: &[u8],
    context: ComposerContextId,
    job: Option<JobId>,
) -> Result<Submission, Refusal> {
    let key = submissions::idempotency_key(request.headers())?;
    let value: Value = serde_json::from_slice(body).map_err(|_| INVALID)?;
    let normalized = serde_json::to_vec(&value).map_err(|_| INVALID)?;
    let mut hash = Context::new(&SHA256);
    hash.update(domain);
    hash.update(&context.get().to_le_bytes());
    if let Some(id) = job {
        hash.update(&id.get().to_le_bytes());
    }
    hash.update(&normalized);
    let digest: [u8; 32] = hash.finish().as_ref().try_into().map_err(|_| INTERNAL)?;
    Ok(Submission {
        key,
        fingerprint: RequestFingerprint::from_digest(digest),
    })
}
fn refusal(error: DatabaseError) -> Refusal {
    match error {
        DatabaseError::Rejected(Violation::InvalidValue("execution_context", reason)) => {
            UNSUPPORTED_CONTEXT.with_reason(reason.to_owned())
        }
        DatabaseError::Rejected(Violation::InvalidValue("allocation_format", _)) => UNAVAILABLE,
        DatabaseError::Rejected(Violation::InvalidValue(
            "revision" | "job" | "allocation" | "context" | "conversation",
            _,
        ))
        | DatabaseError::Rejected(Violation::SubmissionKeyUsed { .. }) => CONFLICT,
        DatabaseError::Rejected(Violation::NotFound(_)) => NOT_FOUND,
        DatabaseError::Rejected(reason) => INVALID.with_reason(reason.to_string()),
        DatabaseError::OutcomeUnknown { .. } => UNCERTAIN,
        other => other.into(),
    }
}
pub(super) fn phase(record: &MutableJobRecord) -> &'static str {
    match record.outcome.as_ref().map(|outcome| outcome.kind) {
        Some(OutcomeKind::Completed) => "completed",
        Some(OutcomeKind::Failed) => "failed",
        Some(OutcomeKind::Cancelled) => "cancelled",
        Some(OutcomeKind::Unknown) => "unknown",
        None => match record.job.phase {
            MutableJobPhase::Unsubmitted => "unsubmitted",
            MutableJobPhase::Queued => "queued",
            MutableJobPhase::Executing => "executing",
        },
    }
}
/// The editable input of `job` as the Composer reads it.
fn input_view(job: &crate::db::entities::MutableJob) -> Value {
    let priority = match job.input.priority {
        JobQueuePriority::Foreground => "foreground",
        JobQueuePriority::Automatic => "automatic",
        JobQueuePriority::Speculative => "speculative",
    };
    match &job.input.content {
        MutableJobContent::Generative {
            text,
            attachments,
            configuration,
        } => {
            let configuration = configuration.map(|config| serde_json::json!({"llm_profile": config.llm_profile.to_string(), "agent_profile": config.agent_profile.map(|id| id.to_string()), "reasoning_effort": config.reasoning_effort.map(|effort| effort.as_str())}));
            serde_json::json!({"kind":"generative", "text": text, "attachments": attachments.iter().map(ToString::to_string).collect::<Vec<_>>(), "configuration": configuration, "priority": priority})
        }
        MutableJobContent::Decision {
            request, profile, ..
        } => {
            serde_json::json!({"kind":"decision", "decision_request":request, "decision_profile":profile.to_string(), "priority":priority})
        }
    }
}
fn view(record: MutableJobRecord) -> Value {
    let phase = phase(&record);
    let input = input_view(&record.job);
    let project = record.project;
    let job = record.job;
    envelope(
        serde_json::json!({"job": {"id":job.id.to_string(), "conversation":job.parent_id.to_string(), "project":project.map(|id| id.to_string()), "previous":job.previous.map(|id| id.to_string()), "context":job.composer_context.to_string(), "revision":job.revision, "phase":phase, "created_at":job.created_at.as_unix_micros(), "submitted_at":job.submitted_at.map(|at| at.as_unix_micros()), "input":input}}),
    )
}

/// Adds the editable state of a Job to its `GET /api/v1/jobs/{job}` body: the
/// Composer context it belongs to, its submission time and its editable
/// input. When the request named a Composer `context`, a queued Generative
/// Job also reports whether it is blocked behind Decision history.
pub(super) async fn add_editable_state(
    body: &mut Value,
    database: &DatabaseHandle,
    context: Option<ComposerContextId>,
    record: &MutableJobRecord,
) -> Result<(), Refusal> {
    let job = &record.job;
    body["context"] = serde_json::json!(job.composer_context.to_string());
    body["submitted_at"] = serde_json::json!(job.submitted_at.map(|at| at.as_unix_micros()));
    body["input"] = input_view(job);
    if let Some(context) = context {
        let queued_generative = job.phase == MutableJobPhase::Queued
            && record.outcome.is_none()
            && matches!(job.input.content, MutableJobContent::Generative { .. });
        body["unsupported_decision_history"] = Value::Bool(if queued_generative {
            database
                .mutable_job_with_support(context, job.id)
                .await
                .map_err(refusal)?
                .1
        } else {
            false
        });
    }
    Ok(())
}
pub(super) enum Action {
    Create,
    Current,
    Edit(JobId),
    Submit(JobId),
    CancelQueued(JobId),
    Discard(JobId),
}

pub(super) async fn respond(
    action: Action,
    request: &HttpRequest,
    payload: web::types::Payload,
    database: &DatabaseHandle,
) -> Result<Reply, Refusal> {
    let context = context(request)?;
    match action {
        Action::Create => {
            let body = read_body(payload, MAX_SUBMISSION_BYTES).await?;
            let input: Input = serde_json::from_slice(&body).map_err(|_| INVALID)?;
            let conversation = input
                .conversation
                .as_deref()
                .map(routing::job_id)
                .transpose()?;
            let project = input
                .project
                .as_deref()
                .map(projects::project_id)
                .transpose()?;
            let submission = submission(request, &body, b"mutable-create-v1", context, None)?;
            let record = database
                .create_mutable_job(context, convert(input)?, submission, conversation, project)
                .await
                .map_err(refusal)?;
            Ok(Reply::new(StatusCode::OK, view(record)))
        }
        Action::Current => {
            let record = database
                .mutable_job_for_context(context)
                .await
                .map_err(refusal)?
                .ok_or(NOT_FOUND)?;
            Ok(Reply::ok(view(record)))
        }
        Action::Edit(id) => {
            let body = read_body(payload, MAX_SUBMISSION_BYTES).await?;
            let change: Change = serde_json::from_slice(&body).map_err(|_| INVALID)?;
            if change.input.conversation.is_some() || change.input.project.is_some() {
                return Err(INVALID);
            }
            let job = database
                .update_mutable_job(context, id, change.revision, convert(change.input)?)
                .await
                .map_err(refusal)?;
            Ok(Reply::ok(view(job)))
        }
        Action::Submit(id) => {
            let body = read_body(payload, MAX_SUBMISSION_BYTES).await?;
            let change: Submit = serde_json::from_slice(&body).map_err(|_| INVALID)?;
            if change
                .input
                .as_ref()
                .is_some_and(|input| input.conversation.is_some() || input.project.is_some())
            {
                return Err(INVALID);
            }
            let submission = submission(request, &body, b"mutable-submit-v1", context, Some(id))?;
            let job = database
                .submit_mutable_job(
                    context,
                    id,
                    change.revision,
                    change.input.map(convert).transpose()?,
                    submission,
                )
                .await
                .map_err(refusal)?;
            Ok(Reply::ok(view(job)))
        }
        Action::Discard(id) => {
            let name = header::HeaderName::from_static("if-match");
            let revision = single(request.headers(), &name)
                .and_then(|v| v.to_str().ok())
                .and_then(decimal::<u64>)
                .ok_or(INVALID)?;
            database
                .discard_mutable_job(context, id, revision)
                .await
                .map_err(refusal)?;
            Ok(Reply::ok(envelope(
                serde_json::json!({"discarded":id.to_string()}),
            )))
        }
        Action::CancelQueued(id) => {
            let body = read_body(payload, MAX_SUBMISSION_BYTES).await?;
            let input: CancelQueued = serde_json::from_slice(&body).map_err(|_| INVALID)?;
            let job = database
                .cancel_unsupported_queued_job(context, id, input.revision)
                .await
                .map_err(refusal)?;
            Ok(Reply::ok(view(job)))
        }
    }
}
