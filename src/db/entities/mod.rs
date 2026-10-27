use std::collections::BTreeSet;
use std::fmt;
use std::num::NonZeroU32;
use std::time::{SystemTime, UNIX_EPOCH};

use crate::service::ai::providers::openai_compatible::{
    ChatFinishReason, NormalizedUsage, ProviderFailure, ToolCallRefusal,
};

mod approvals;
mod attachments;
mod identity;
mod job_context;
mod jobs;
mod metrics;
mod operations;
pub(crate) mod output_filter;
mod profiles;
mod projects;
mod requests;
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
pub(in crate::db) use approvals::{MAX_RECONCILER_BYTES, MAX_RECONCILIATION_TEXT_BYTES};
pub use attachments::*;
pub use identity::{
    AttachmentId, AvailabilityStatId, ConfigId, ExecutionPoolId, ExecutorId, JobId, JobInputId,
    JobOutputId, LlmProviderId, MetricsId, ProfileId, ProjectId, RecordRef, RequestEventId,
    RequestNumber, RequestRef, Timestamp, parse_entity_id,
};
pub(crate) use job_context::parse_suggested_title;
pub use job_context::{
    Actor, Appended, COMPACTION_JOB_NAME, CompactionMode, CompactionPolicy, CompactionPreset,
    CompactionRequest, ContextSourceManifest, CustomPolicy, Entry, EntryKind,
    GlobalCompactionSettings, JobAction, JobEdit, JobGroup, JobInputEntry, JobLimits,
    MAX_JOB_DEPTH, Notice, NoticeBody, NoticeClass, NoticeEvent, Origin, PredictionAcceptance,
    PredictionMode, PredictiveCompaction, RecordEvent, Recurrence, RecurrenceHalt, RecurrencePhase,
    ResolvedCompaction, ResponseFormat, SUMMARIZER_OUTPUT_RESERVE, SourceBoundary, StopCause,
    StopProof, SummarizationResult, SummarizationStage, SummarizationWork, SummaryFragment,
    TITLE_JOB_NAME, title_prompt,
};
pub(in crate::db) use job_context::{effective_entries, summary_chunks};
pub use jobs::*;
pub use metrics::*;
pub use operations::{MAX_PENDING_JOBS, OperationId, OperationKind, Violation};
pub use profiles::*;
pub use projects::*;
pub use requests::*;
pub use scheduling::*;
pub use statistics::{
    AvailabilityClassification, AvailabilityObservation, AvailabilityRequestSample,
    AvailabilityStat, AvailabilityStatRecord, MixedRequests, ObservationCursor, ObservationFilter,
    ObservationPage, RejectionCause, TimeRange, ToolStatRecord, UnavailableCause,
};
pub(crate) use usage::{JobUsage, TokenEvidence, TokenObservation};
pub use usage::{
    RequestEligibility, TokenAnnotation, TokenCount, TokenRequestSample, TokenUsageState,
    USAGE_SUMMARY_HOUR_MICROS, UsageBucketResponse, UsageBucketRow, UsageBucketWidth,
    UsageBucketing, UsageCursor, UsageFinality, UsageRetryRow, UsageSummaryFilter,
    UsageSummaryResponse, UsageSummaryRow,
};

pub(crate) use crate::db::derived::{JobActivity, JobActivityPhase, JobPlanStepView, JobPlanView};
