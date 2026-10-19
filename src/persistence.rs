use std::collections::BTreeSet;
use std::fmt;
use std::num::NonZeroU32;
use std::time::{SystemTime, UNIX_EPOCH};

use crate::openai_compatible::{
    ChatFinishReason, MAX_TOOL_CALL_ID_BYTES, MAX_TOOL_DEFINITION_BYTES, NormalizedUsage,
    ProviderFailure, ToolCallRefusal, is_function_name,
};
use crate::storage::{Batch, BatchError};
use crate::token_usage::{RequestEligibility, TokenAnnotation, UsageFinality};

mod availability;
mod execution;
mod identity;
mod mutations;
mod operations;
mod provider_record;
mod records;
pub use provider_record::{MessageBody, NewProviderAnswer, NewSentRequest, ProviderRequestRecord};
pub(crate) use provider_record::{record_provider_answer, record_sent_request};
mod scheduling;
mod tool_evidence;

pub use availability::{
    AvailabilityClassification, AvailabilityObservation, AvailabilityRequestSample,
    AvailabilityStatRecord, MixedRequests, NewAvailabilityStat, ObservationCursor,
    ObservationFilter, ObservationPage, ObservedModel, ObservedModelChoice, ObservedModelFilter,
    ObservedModelPage, RejectionCause, TimeRange, UnavailableCause,
};
pub use execution::{
    AppendedOutput, DependencyCondition, DependencyState, ExecutionConfiguration, FinalResult,
    IntermediateOutputKind, JobFinalization, JobInputDraft, JobOutcome, JobRetry, NewExecutor,
    NewJob, NewJobOutput, OutputKind,
};
pub use identity::{
    AttachmentId, AvailabilityStatId, ConfigId, ExecutionPoolId, ExecutorId, JobId, JobInputId,
    JobOutputId, LlmProviderId, MetricsId, ProfileId, ProjectId, RecordRef, RequestNumber,
    RequestRef, Timestamp,
};
pub(crate) use mutations::{
    PublishedAttachment, append_job_output, associate_job_project, check_new_attachment,
    claim_execution, cleanup_attachment, confirm_job_stopped, create_attachment, create_config,
    create_execution_pool, create_executor, create_job, create_project, create_submitted_job,
    finalize_job, reconcile_tool_effect, record_availability_stat, record_metrics,
    record_tool_decision, record_tool_outcome, retry_job, set_execution_pool_enabled,
    update_config, update_execution_pool,
};
pub use mutations::{is_handler_version, sanitize_display_name};
pub use operations::{MAX_PENDING_JOBS, OperationId, OperationKind, Violation};
pub use records::{
    AgentBehavior, AgentConfig, AttachmentCleanup, AttachmentOrigin, AttachmentRecord,
    CapabilitySet, ClaimRecord, CleanupKind, ConfigBody, ConfigKind, ConfigRecord, ConfigRef,
    ConnectionConfig, ContentDigest, ExecutionOperation, ExecutionPool, ExecutionPoolConfig,
    ExecutorRecord, FrozenExecutionConfig, FrozenLlmConfig, JobInputRecord, JobOutputRecord,
    JobRecord, JobStateRecord, LlmConfig, LlmParameters, LlmTarget, LlmTransport,
    MAX_ATTACHMENT_BYTES, MAX_ATTACHMENT_MIME_BYTES, MAX_ATTACHMENT_NAME_BYTES, Measurements,
    MetricsRecord, ModelCapability, ModelConfig, NewAttachment, NewConfig, NewExecutionPool,
    NewMetrics, NewProject, NewSubmission, OutcomeCause, OutcomeKind, OutcomeRecord, PendingJobKey,
    PendingJobsPage, PoolConfig, ProjectRecord, ProviderSnapshot, RequestEventRecord,
    RequestFingerprint, RequestPayload, RequestPayloadKind, SubmissionKey, SubmissionRecord,
    SubmittedConfiguration, SuspensionRecord, ToolCallRecord,
};
pub use scheduling::*;
pub use tool_evidence::{
    AuthorizationBasis, EffectConclusion, MAX_HANDLER_VERSION_BYTES,
    MAX_STORED_TOOL_ARGUMENTS_BYTES, MAX_STORED_TOOL_ARGUMENTS_PER_JOB_BYTES,
    MAX_STORED_TOOL_SCHEMAS_PER_JOB_BYTES, NewToolEvidence, NotInvokedReason, Recorded,
    ToolAdmission, ToolCallRef, ToolDecision, ToolEffect, ToolEvidenceRecord, ToolOutcome,
    ToolOutcomeKind, ToolPayload, ToolReconciliation, UncertainCause, WithheldReason,
};
use tool_evidence::{MAX_RECONCILER_BYTES, MAX_RECONCILIATION_TEXT_BYTES};

#[cfg(test)]
mod classification_tests {
    use super::*;

    fn observation(
        status_code: Option<u16>,
        finish_reason: Option<ChatFinishReason>,
        raw_finish_reason: Option<&str>,
        failure: Option<&str>,
    ) -> AvailabilityObservation {
        AvailabilityObservation {
            status_code,
            finish_reason,
            raw_finish_reason: raw_finish_reason.map(str::to_owned),
            failure: failure.map(|message| ProviderFailure {
                message: message.to_owned(),
                status_code,
                ..ProviderFailure::default()
            }),
        }
    }

    #[test]
    fn classification_follows_only_observed_status_and_finish_facts() {
        use AvailabilityClassification::{Available, Rejected, Unavailable, Unconfirmed};
        use ChatFinishReason::{ContentFilter, Error, Length, Other, Stop, ToolCalls};
        let cases = [
            // HTTP 200 needs a normal terminal finish reason.
            (observation(Some(200), None, None, None), Unconfirmed),
            (
                observation(Some(200), Some(Stop), Some("stop"), None),
                Available,
            ),
            (observation(Some(200), Some(Stop), None, None), Available),
            (
                observation(Some(200), Some(Length), Some("length"), None),
                Available,
            ),
            (
                observation(Some(201), Some(Stop), Some("stop"), None),
                Unconfirmed,
            ),
            (
                observation(Some(200), Some(ToolCalls), Some("tool_calls"), None),
                Unconfirmed,
            ),
            (
                observation(Some(200), Some(ContentFilter), Some("content_filter"), None),
                Unconfirmed,
            ),
            (
                observation(Some(200), Some(Error), Some("error"), None),
                Unconfirmed,
            ),
            (
                observation(Some(200), Some(Other), Some("other"), None),
                Unconfirmed,
            ),
            // Conflicting facts are not evidence of completion.
            (
                observation(Some(200), Some(Stop), Some("length"), None),
                Unconfirmed,
            ),
            (
                observation(Some(200), Some(Stop), Some("stop"), Some("stream reset")),
                Unconfirmed,
            ),
            (
                observation(Some(200), None, Some("stop"), None),
                Unconfirmed,
            ),
            (
                observation(None, Some(Stop), Some("stop"), None),
                Unconfirmed,
            ),
            // Provider HTTP 5xx is unavailability.
            (
                observation(Some(500), None, None, Some("server error")),
                Unavailable(UnavailableCause::ProviderServerError),
            ),
            (
                observation(Some(503), None, None, Some("overloaded")),
                Unavailable(UnavailableCause::ProviderServerError),
            ),
            (
                observation(Some(599), None, None, None),
                Unavailable(UnavailableCause::ProviderServerError),
            ),
            // Definite refusals are kept apart by cause.
            (
                observation(Some(429), None, None, Some("rate limited")),
                Rejected(RejectionCause::RateLimited),
            ),
            (
                observation(Some(401), None, None, Some("bad key")),
                Rejected(RejectionCause::AuthOrPermission),
            ),
            (
                observation(Some(403), None, None, Some("forbidden")),
                Rejected(RejectionCause::AuthOrPermission),
            ),
            (
                observation(Some(400), None, None, Some("bad body")),
                Rejected(RejectionCause::RequestOrTarget),
            ),
            (
                observation(Some(404), None, None, Some("no model")),
                Rejected(RejectionCause::RequestOrTarget),
            ),
            (
                observation(Some(422), None, None, Some("invalid")),
                Rejected(RejectionCause::RequestOrTarget),
            ),
            // Statuses with no approved mapping are not invented into a class.
            (
                observation(Some(408), None, None, Some("timeout")),
                Unconfirmed,
            ),
            (observation(Some(302), None, None, None), Unconfirmed),
            // A transport failure without an observed Provider status is not proof.
            (
                observation(None, None, None, Some("connection reset")),
                Unconfirmed,
            ),
            (observation(None, None, None, None), Unconfirmed),
        ];
        for (observation, expected) in cases {
            assert_eq!(
                observation.classification(),
                expected,
                "classifying {observation:?}"
            );
        }
    }
}
