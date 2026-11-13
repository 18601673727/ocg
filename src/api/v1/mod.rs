use std::fmt;
use std::net::{Ipv4Addr, SocketAddr, TcpListener};
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::time::Duration;

use aws_lc_rs::constant_time::verify_slices_are_equal;
use aws_lc_rs::digest::{SHA256, digest};
use ntex::http::header::{self, HeaderMap, HeaderValue};
use ntex::http::{HttpServiceConfig, Method, StatusCode};
use ntex::io::IoConfig;
use ntex::server::Server;
use ntex::service::cfg::SharedCfg;
use ntex::time::Seconds;
use ntex::web::{self, App, HttpRequest, HttpResponse, HttpServer};
use serde_json::{Value, json};

use crate::db::entities::{
    AttachmentId, ExecutionPoolId, JobId, JobOutputRecord, NotInvokedReason, OutputKind, ProfileId,
    ProjectId, RequestNumber, ToolDecision, ToolEvidenceRecord, ToolOutcome, ToolOutcomeKind,
    UncertainCause, WithheldReason,
};
use crate::db::{DatabaseError, DatabaseHandle, DatabaseStatus};
use crate::system::scheduler::SchedulerHandle;

mod assets;
mod attachments;
mod browser;
mod catalog;
mod config;
pub mod contracts;
mod dashboard;
mod events;
mod jobs;
mod listener;
mod mutable_jobs;
mod outputs;
mod pools;
mod profiles;
mod projects;
mod responses;
mod routing;
mod security;
mod submissions;
mod subscriptions;
mod system;
mod tools;
mod update;
pub(crate) mod wire;

pub(crate) use attachments::AttachmentView;
pub(crate) use browser::SessionResponseView;
pub(crate) use catalog::{
    AgentProfilesView, ChildrenView, ContextStatusView, ConversationEditedView,
    ConversationLimitsView, ConversationsView, JobInputView, TreeView, TurnsView,
};
pub use config::{
    API_VERSION, ConfigError, ControlPlaneConfig, MAX_ATTACHMENT_UPLOAD_BYTES, MAX_OUTPUT_PAGE,
    MAX_SUBMISSION_BYTES, MIN_TOKEN_CHARS, PORT_VARIABLE, TOKEN_VARIABLE, UI_VARIABLE,
};
use config::{
    DEFAULT_OUTPUT_PAGE, MAX_HEADER_BYTES, MAX_HEADERS, MAX_PATH_BYTES, MAX_QUERY_BYTES, Token,
};
pub(crate) use dashboard::{
    AvailabilityView, LogsView, UsageBucketsView, UsageRetriesView, UsageSummaryView, UsageView,
};
pub(crate) use events::{JobEventView, OutputEventView, SettlementEventView};
pub(crate) use jobs::{JobDetailsView, JobsView};
pub use listener::{ControlPlane, StartError, StopError};
pub(crate) use outputs::OutputsView;
use outputs::outputs;
pub(crate) use pools::{PoolAnswerView, PoolsView};
pub(crate) use profiles::{ImportedModelsView, ProfilesView, ProviderModelsView};
pub(crate) use projects::{ProjectAnswerView, ProjectsView};
pub(crate) use responses::ProviderResponsesView;
use routing::{Route, authorize, decimal, respond, route};
pub(crate) use submissions::{
    AdmissionResponseView, AppendView, ApprovalDecisionView, CardsView, CompactionView, StopView,
};
use system::health;
pub(crate) use system::{ActiveSystemSettingsView, HealthView};
pub(crate) use tools::ToolsView;
pub(crate) use update::UpdateView;

use security::{Caller, Destination, admit};
use wire::{
    BODY_NOT_ACCEPTED, BODY_TOO_LARGE, DATABASE_BUSY, DATABASE_UNAVAILABLE, FORBIDDEN_HOST,
    FORBIDDEN_ORIGIN, HEADERS_TOO_LARGE, INCONSISTENT_RECORD, INTERNAL, INVALID_REQUEST,
    JOB_NOT_FOUND, LENGTH_REQUIRED, METHOD_NOT_ALLOWED, NOT_FOUND, ONLY_POST, OUTPUT_TOO_LARGE,
    PROFILE_METHODS, Refusal, Reply, SESSION_METHODS, SHUTTING_DOWN, UNAUTHENTICATED,
    UNSUPPORTED_MEDIA_TYPE, URI_TOO_LONG, answer, read_body, single, submission_body,
};

struct Shared {
    database: DatabaseHandle,
    scheduler: SchedulerHandle,
    submissions: submissions::Registry,
    workspace_tools: crate::system::executor::execution_runtime::WorkspaceTools,
    token: Token,
    port: u16,
    response_bytes: usize,
    admission_wait: Duration,
    stream_limits: events::StreamLimits,
    assets: Option<assets::UiAssets>,
    browser: Option<browser::State>,
    streams: AtomicUsize,
    closing: AtomicBool,
}
