use std::collections::BTreeSet;
use std::fmt;
use std::num::NonZeroU32;
use std::time::{SystemTime, UNIX_EPOCH};

use crate::service::ai::providers::openai_compatible::{
    ChatFinishReason, NormalizedUsage, ProviderFailure, ToolCallRefusal,
};

mod approvals;
mod attachments;
mod execution;
mod headers;
mod identity;
mod job_context;
mod jobs;
mod metrics;
mod operations;
pub(crate) mod output_filter;
mod profiles;
mod projects;
mod requests;
mod scheduled_external;
mod scheduling;
mod statistics;
mod usage;

pub use approvals::{
    ApprovalDecision, ApprovalRecord, AuthorizationBasis, EffectConclusion,
    MAX_HANDLER_VERSION_BYTES, MAX_STORED_TOOL_ARGUMENTS_BYTES, NotInvokedReason, Recorded,
    ToolAdmission, ToolCallRef, ToolDecision, ToolEffect, ToolEvidence, ToolEvidenceRecord,
    ToolOutcome, ToolOutcomeKind, ToolPayload, ToolReconciliation, UncertainCause, WithheldReason,
    is_handler_version,
};
pub use attachments::*;
pub(crate) use execution::{ExecutionEvent, ExecutionEventKind, ExecutionSnapshot};
pub use headers::*;
pub use identity::{
    AttachmentId, AvailabilityStatId, ComposerContextId, ConfigId, ExecutionPoolId, ExecutorId,
    JobId, JobInputId, JobOutputId, LlmProviderId, MetricsId, ProfileId, ProjectId, RecordRef,
    RequestEventId, RequestNumber, RequestRef, ReservationRequestId, ScheduledExternalPolicyId,
    Timestamp, parse_entity_id,
};
pub(in crate::db) use job_context::summary_chunks;
pub use job_context::{
    Actor, Appended, COMPACTION_JOB_NAME, CompactionMode, CompactionPolicy, CompactionPreset,
    CompactionRequest, ContextSourceManifest, CustomPolicy, Entry, EntryKind,
    GlobalCompactionSettings, JobAction, JobEdit, JobGroup, JobLimits, MAX_JOB_DEPTH,
    NativeSummaryBoundary, NativeSummaryManifest, NativeSummaryResultLink, Origin,
    PredictionAcceptance, PredictionMode, PredictiveCompaction, Recurrence, RecurrenceHalt,
    RecurrencePhase, ResolvedCompaction, ResponseFormat, SUMMARIZER_OUTPUT_RESERVE, SourceBoundary,
    StopCause, StopProof, SummarizationResult, SummarizationStage, SummarizationWork,
    SummaryFragment, TITLE_JOB_NAME, title_prompt,
};
pub use jobs::*;
pub use metrics::*;
pub use operations::{MAX_PENDING_JOBS, OperationId, OperationKind, Violation};
pub use profiles::*;
pub use projects::*;
pub use requests::*;
pub use scheduled_external::*;
pub use scheduling::*;
pub use statistics::{
    AvailabilityClassification, AvailabilityObservation, AvailabilityRequestSample,
    AvailabilityStat, AvailabilityStatRecord, DecisionTarget, MixedRequests, ObservationCursor,
    ObservationFilter, ObservationPage, RejectionCause, TimeRange, ToolStatRecord,
    UnavailableCause,
};
pub(crate) use usage::{JobUsage, TokenEvidence, TokenObservation};
pub use usage::{
    RequestEligibility, TokenAnnotation, TokenCount, TokenRequestSample, TokenUsageState,
    USAGE_SUMMARY_HOUR_MICROS, UsageBucketResponse, UsageBucketRow, UsageBucketWidth,
    UsageBucketing, UsageCursor, UsageFinality, UsageRetryRow, UsageSummaryFilter,
    UsageSummaryResponse, UsageSummaryRow,
};

pub(crate) use crate::db::derived::{JobActivity, JobActivityPhase, JobPlanStepView, JobPlanView};
