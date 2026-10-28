//! `ocg workspace-read <objective>`: the executable's opt-in, one-shot Job
//! that may read files of a Workspace the operator authorized, and
//! `ocg workspace-edit <objective>`, its counterpart that may also create
//! and conditionally update them, and `ocg workspace-code <objective>`, which
//! may also build and test the Workspace. `ocg workspace-vision <image-path>
//! <objective>` instead stores one PNG or JPEG image of the Workspace as an
//! Attachment and sends it with the objective, offering no tool. This module
//! belongs to the executable, not to the library.
//!
//! It runs the ordinary host and submits exactly one Job through the host's
//! scheduler, offering only `workspace_read_file`; for `workspace-edit`,
//! `workspace_read_file` and `workspace_write_file`; for `workspace-code`,
//! those, `workspace_run_command`, `workspace_list_directory` and
//! `workspace_search`; all bound to the root of the Project named by
//! [`PROJECT_VARIABLE`], which the Job is submitted with, and
//! `workspace_run_command` offering that Project's commands. Writing is offered by the last two commands
//! alone, and running commands by `workspace-code` alone; every write is
//! recorded, with its contents, and every command's output in plaintext tool
//! evidence. An image is published to the attachment store of the data
//! directory and recorded as a user-provided Attachment before the Job is
//! submitted; the Job's input names it, and the execution reads it back from
//! there, so the Workspace file is not needed once it is stored. The
//! Provider and model are named by the environment too. The credential is
//! named only by reference: the existing resolver reads it when the request
//! is built, and it is never stored, printed or logged. Once the Job
//! settles, what the database recorded of it is printed and the host stops.
//! A signal while the Job runs cancels it, and the host stops once it has
//! settled.

use std::cell::RefCell;
use std::collections::{BTreeMap, BTreeSet};
use std::fmt::Write as _;
use std::path::Path;
use std::rc::Rc;
use std::sync::Arc;

use ntex::util::{Either, select};

use crate::db::attachments::AttachmentError;
use crate::db::entities::{
    Attachment, AvailabilityStatRecord, ContentDigest, ExecutionConfiguration, Job, JobId,
    JobInputDraft, MetricsRecord, ModelCapability, OutputKind, ProjectId, RequestNumber,
    ToolDecision, ToolOutcome, parse_entity_id,
};
use crate::db::entities::{TokenCount, TokenRequestSample};
use crate::db::{DatabaseError, DatabaseHandle};
#[cfg(unix)]
use crate::service::ai::command;
use crate::service::ai::providers::openai_compatible::image::MAX_IMAGE_BYTES;
use crate::service::ai::providers::openai_compatible::{ImageError, ImageInput, ImageMime};
use crate::service::ai::providers::{
    CancellationToken, EnvironmentCredentialResolver, ProviderExecutionConfig,
};
use crate::system::data_directory::DataDirectory;
use crate::system::executor::ToolRegistryError;
use crate::system::executor::execution_runtime::{
    AdmissionError, ExecutionRuntimeError, ProviderExecutionPlan, ToolExecution,
};
use crate::system::executor::workspace_read::{self, Refusal, Workspace, WorkspaceError};
use crate::system::executor::{workspace_discovery, workspace_write};
use crate::system::runtime::{self, HostContext, HostError};

use super::llm_target::{Target, targets};

/// The command that selects the read-only mode, as the executable's only
/// argument before the objective.
pub const COMMAND: &str = "workspace-read";
/// The command that selects the mode that may also write files.
#[cfg(unix)]
pub const EDIT_COMMAND: &str = "workspace-edit";
/// The command that selects the mode that may also run build and test
/// commands.
#[cfg(unix)]
pub const CODE_COMMAND: &str = "workspace-code";
/// The command that selects the mode that sends one image with the
/// objective.
pub const VISION_COMMAND: &str = "workspace-vision";
pub use super::llm_target::{CREDENTIAL_REF_VARIABLE, ENDPOINT_VARIABLE, MODEL_VARIABLE};
/// The Project the Job works in, identified by its UUID: its root is the
/// Workspace the Job may read from.
pub const PROJECT_VARIABLE: &str = "OCG_PROJECT";

/// What the one Job may do in its Workspace, as its command chose.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Mode {
    /// Read files: `workspace_read_file` alone.
    Read,
    /// Read, create and conditionally update files: `workspace_read_file`
    /// and `workspace_write_file`.
    #[cfg(unix)]
    Edit,
    /// Read, write, build and test, and discover files:
    /// `workspace_read_file`, `workspace_write_file`,
    /// `workspace_run_command`, `workspace_list_directory` and
    /// `workspace_search`.
    #[cfg(unix)]
    Code,
    /// Send one image of the Workspace with the objective; no tool.
    Vision,
}

impl Mode {
    /// The mode `command` selects, if it selects one.
    pub fn of(command: &str) -> Option<Self> {
        match command {
            COMMAND => Some(Self::Read),
            #[cfg(unix)]
            EDIT_COMMAND => Some(Self::Edit),
            #[cfg(unix)]
            CODE_COMMAND => Some(Self::Code),
            VISION_COMMAND => Some(Self::Vision),
            _ => None,
        }
    }

    /// The command of the mode. It also names the Executor every Job of the
    /// mode runs on, which is registered once and reused, as are the
    /// Provider and the LLM Profile of a configuration.
    pub fn command(self) -> &'static str {
        match self {
            Self::Read => COMMAND,
            #[cfg(unix)]
            Self::Edit => EDIT_COMMAND,
            #[cfg(unix)]
            Self::Code => CODE_COMMAND,
            Self::Vision => VISION_COMMAND,
        }
    }

    /// Whether the command takes an image path before the objective.
    pub fn takes_image(self) -> bool {
        self == Self::Vision
    }

    /// The error that shows how the mode's command is used.
    pub fn usage(self) -> Error {
        if self.takes_image() {
            Error::ImageUsage(self.command())
        } else {
            Error::Usage(self.command())
        }
    }

    /// What the mode's Model is declared to be capable of.
    fn capabilities(self) -> BTreeSet<ModelCapability> {
        let used = if self.takes_image() {
            ModelCapability::Vision
        } else {
            ModelCapability::Tools
        };
        BTreeSet::from([ModelCapability::Chat, used])
    }
}

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("{0} must be set")]
    NotConfigured(&'static str),
    #[error("{PROJECT_VARIABLE} must name a Project by its UUID")]
    InvalidProject,
    #[error("{0} does not exist")]
    ProjectMissing(ProjectId),
    #[error("the root of the Project is no longer the directory it was created with")]
    ProjectRootChanged,
    #[error("{ENDPOINT_VARIABLE} must be an https:// URL")]
    NotHttps,
    #[error(
        "OCG_PROVIDER_PROTOCOL must be openai_chat_completions, openai_responses or anthropic_messages"
    )]
    InvalidProtocol,
    #[error("the variable named by {CREDENTIAL_REF_VARIABLE} is not set")]
    CredentialMissing,
    #[error("usage: ocg {0} <objective>")]
    Usage(&'static str),
    #[error("usage: ocg {0} <image-path> <objective>")]
    ImageUsage(&'static str),
    #[error("the image cannot be sent: {0}")]
    Image(String),
    #[error("the image was not stored: {0}")]
    Attachment(#[from] AttachmentError),
    #[error("workspace_read_file is not enabled: {0}")]
    Workspace(#[from] WorkspaceError),
    #[error("workspace authorization worker: {0}")]
    WorkspaceWorker(String),
    #[error("workspace_read_file cannot be offered: {0}")]
    Tools(#[from] ToolRegistryError),
    #[error("database: {0}")]
    Database(#[from] DatabaseError),
    #[error("the Job was not settled: {0}")]
    NotSettled(ExecutionRuntimeError),
    #[error("Job {job} did not complete; it is {outcome}")]
    NotCompleted { job: JobId, outcome: &'static str },
    #[error("the host stopped before the Job was submitted")]
    NotRun,
    #[error(transparent)]
    Host(#[from] HostError),
}

/// What the operator chose for one run.
pub struct Config {
    mode: Mode,
    /// The image to send, relative to the Workspace root, for a mode that
    /// takes one.
    image: Option<String>,
    project: ProjectId,
    protocol: crate::db::entities::LlmTransport,
    endpoint: String,
    model: String,
    credential_ref: String,
    objective: String,
}

impl Config {
    /// Reads the configuration from the environment, with the `mode`, the
    /// `image` path for a mode that takes one, and the `objective` given on
    /// the command line. The credential itself is only checked to exist.
    #[allow(clippy::result_large_err)]
    pub fn from_environment(
        mode: Mode,
        image: Option<String>,
        objective: Option<String>,
    ) -> Result<Self, Error> {
        let objective = objective
            .filter(|objective| !objective.trim().is_empty())
            .ok_or_else(|| mode.usage())?;
        let image = image.filter(|image| !image.is_empty());
        if image.is_some() != mode.takes_image() {
            return Err(mode.usage());
        }
        let variable = |name: &'static str| {
            std::env::var(name)
                .ok()
                .filter(|value| !value.trim().is_empty())
                .ok_or(Error::NotConfigured(name))
        };
        let project = variable(PROJECT_VARIABLE)
            .ok()
            .and_then(|value| parse_entity_id(&value))
            .map(ProjectId::new)
            .ok_or(Error::InvalidProject)?;
        let endpoint = variable(ENDPOINT_VARIABLE)?;
        if !endpoint.starts_with("https://") {
            return Err(Error::NotHttps);
        }
        let protocol =
            super::llm_target::protocol_from_environment().map_err(|_| Error::InvalidProtocol)?;
        let model = variable(MODEL_VARIABLE)?;
        let credential_ref = variable(CREDENTIAL_REF_VARIABLE)?;
        if std::env::var_os(&credential_ref).is_none_or(|credential| credential.is_empty()) {
            return Err(Error::CredentialMissing);
        }
        Ok(Self {
            mode,
            image,
            project,
            protocol,
            endpoint,
            model,
            credential_ref,
            objective,
        })
    }
}

/// Runs the host until the one Job has settled and been reported. Inside
/// it, the Project is read and its root authorized before anything is
/// recorded.
#[allow(clippy::result_large_err)]
pub fn run(data_directory: &DataDirectory, config: Config) -> Result<(), Error> {
    let settled = Rc::new(RefCell::new(None));
    let slot = settled.clone();
    let directory = data_directory.clone();
    let hosted = runtime::run_hosted(data_directory, true, move |context| async move {
        *slot.borrow_mut() = Some(submit(&context, &directory, config).await);
    });
    let settled = settled.borrow_mut().take();
    match (settled, hosted) {
        (Some(Err(error)), _) => Err(error),
        (_, Err(error)) => Err(error.into()),
        (None, Ok(())) => Err(Error::NotRun),
        (Some(Ok(())), Ok(())) => Ok(()),
    }
}

/// What the mode offers: the tools bound to the Project's Workspace, and the
/// image to store when the mode sends one.
#[allow(clippy::result_large_err)]
async fn prepare(
    database: &DatabaseHandle,
    data_directory: &DataDirectory,
    config: &Config,
) -> Result<(Option<ToolExecution>, Option<Attachment>), Error> {
    let project = database
        .project(config.project)
        .await?
        .ok_or(Error::ProjectMissing(config.project))?;
    let authorize_root = project.root.clone();
    let authorize_directory = data_directory.clone();
    let workspace = crate::system::executor::blocking::run(
        crate::system::executor::blocking::Kind::File,
        move || Workspace::authorize(Path::new(&authorize_root), &authorize_directory),
    )
    .await
    .map_err(|reason| Error::WorkspaceWorker(reason.to_owned()))??;
    if workspace.root() != Path::new(&project.root) {
        return Err(Error::ProjectRootChanged);
    }
    let root = project.root.as_str();
    let mut image = None;
    let tools = match config.mode {
        Mode::Read => {
            eprintln!(
                "OCG Workspace authorized for {}: {root}",
                workspace_read::TOOL_NAME
            );
            #[cfg(unix)]
            {
                Some(command::workspace_tools(
                    workspace,
                    command::Grant::Read,
                    &project.commands,
                )?)
            }
            #[cfg(not(unix))]
            {
                let _ = workspace;
                None
            }
        }
        #[cfg(unix)]
        Mode::Edit => {
            eprintln!(
                "OCG Workspace authorized for {} and {}: {root}",
                workspace_read::TOOL_NAME,
                workspace_write::TOOL_NAME
            );
            eprintln!(
                "OCG records the path and complete content of every write, an update or delete's expected content, and a replace's old and new content, in plaintext tool evidence"
            );
            Some(command::workspace_tools(
                workspace,
                command::Grant::Edit,
                &project.commands,
            )?)
        }
        #[cfg(unix)]
        Mode::Code => {
            eprintln!(
                "OCG Workspace authorized for {}, {}, {}, {} and {}: {root}",
                workspace_read::TOOL_NAME,
                workspace_write::TOOL_NAME,
                command::TOOL_NAME,
                workspace_discovery::LIST_TOOL_NAME,
                workspace_discovery::SEARCH_TOOL_NAME
            );
            eprintln!(
                "OCG records the path and complete content of every write, an update or delete's expected content, a replace's old and new content, and every command's output in plaintext tool evidence, and sends command output to the Provider"
            );
            eprintln!(
                "OCG runs `cargo check`, `cargo test` and the Project's commands in the Workspace with its own permissions; build scripts, tests and commands run unsandboxed, so authorize only a Workspace you trust"
            );
            eprintln!("OCG additionally offers lazy bounded FTS.");
            Some(command::workspace_tools(
                workspace,
                command::Grant::Code,
                &project.commands,
            )?)
        }
        Mode::Vision => {
            let path = config
                .image
                .as_deref()
                .ok_or(Error::ImageUsage(VISION_COMMAND))?;
            let loaded = load_image(&workspace, path)?;
            eprintln!("OCG Workspace authorized for one image: {root}");
            eprintln!(
                "OCG stores {path} ({}, {} bytes, sha256 {}) as an Attachment in its data directory and sends it with the objective to the Provider",
                loaded.mime().as_str(),
                loaded.bytes().len(),
                ContentDigest::of(loaded.bytes())
            );
            image = Some(Attachment {
                mime: loaded.mime().as_str().to_owned(),
                content: loaded.bytes().to_vec(),
                display_name: Some(path.to_owned()),
            });
            None
        }
    };
    Ok((tools, image))
}

/// The image at `path` in the Workspace: its type is the one its extension
/// declares, and its bytes, read within the image limit, must be of that
/// type.
#[allow(clippy::result_large_err)]
fn load_image(workspace: &Workspace, path: &str) -> Result<ImageInput, Error> {
    let declared = declared_type(path).ok_or_else(|| {
        Error::Image(
            "only .png, .jpg, .jpeg, .gif and .webp files can be sent as images".to_owned(),
        )
    })?;
    let bytes = workspace
        .read_binary(path, MAX_IMAGE_BYTES)
        .map_err(|refusal| {
            Error::Image(match refusal {
                Refusal::TooLarge { .. } => ImageError::TooLarge.to_string(),
                other => other.to_string(),
            })
        })?;
    ImageInput::new(declared, bytes).map_err(|error| Error::Image(error.to_string()))
}

/// The image type a path's extension declares, if it is one OCG sends.
fn declared_type(path: &str) -> Option<ImageMime> {
    let (_, extension) = path.rsplit_once('.')?;
    match extension.to_ascii_lowercase().as_str() {
        "png" => Some(ImageMime::Png),
        "jpg" | "jpeg" => Some(ImageMime::Jpeg),
        "gif" => Some(ImageMime::Gif),
        "webp" => Some(ImageMime::Webp),
        _ => None,
    }
}

/// Submits the Job, with `image` stored as its Attachment when given,
/// reports it once it has started, waits for it to settle, and prints what
/// was recorded.
#[allow(clippy::result_large_err)]
async fn submit(
    context: &HostContext,
    data_directory: &DataDirectory,
    config: Config,
) -> Result<(), Error> {
    // Registered first, so a signal while the Job is prepared cancels it too.
    let signal = ntex::rt::signals::signal();
    let database = context.database();
    let (tools, image) = prepare(database, data_directory, &config).await?;
    let target = Target {
        name: config.mode.command().to_owned(),
        protocol: config.protocol,
        endpoint: config.endpoint,
        model: config.model,
        credential_ref: config.credential_ref,
        capabilities: config.mode.capabilities(),
    };
    let (executor, llm_profile, pool) = targets(database, &target).await?;
    eprintln!("OCG execution Pool: {pool}");
    let mut attachments = Vec::new();
    if let Some(image) = image {
        attachments.push(database.create_user_attachment(image).await?);
    }
    let command = config.mode.command();
    let cancellation = CancellationToken::default();
    let plan = ProviderExecutionPlan {
        executor_id: executor,
        input: JobInputDraft {
            objective: Some(config.objective),
            attachments,
        },
        job: Job {
            process_grant: match config.mode {
                Mode::Vision => None,
                Mode::Read => Some("read".into()),
                #[cfg(unix)]
                Mode::Edit => Some("edit".into()),
                #[cfg(unix)]
                Mode::Code => Some("code".into()),
            },
            name: Some(format!("{command} one-shot")),
            eligible_at: None,
            parent: database.genesis(),
            project: Some(config.project),
            input: JobInputDraft::default(),
            configuration: Some(ExecutionConfiguration {
                reasoning_effort: None,
                agent_profile: None,
                llm_profile,
            }),
            previous: None,
        },
        credential_resolver: Arc::new(EnvironmentCredentialResolver),
        provider_config: ProviderExecutionConfig {
            cancellation: cancellation.clone(),
            ..ProviderExecutionConfig::default()
        },
        tools,
    };
    let scheduler = context.scheduler();
    let execution = async {
        let admitted = scheduler
            .admit_provider(plan)
            .await
            .map_err(AdmissionError::into_inner)?;
        eprintln!("OCG started Job {}", admitted.job());
        admitted.settled().await
    };
    let mut scheduled = std::pin::pin!(execution);
    let settled = match select(signal, scheduled.as_mut()).await {
        Either::Right(settled) => settled,
        Either::Left(_) => {
            eprintln!("OCG received a signal; cancelling the Job");
            cancellation.cancel();
            scheduled.await
        }
    };
    let (job, problem) = match settled {
        Ok(receipt) => (receipt.job, None),
        Err(error) => match settled_job(&error) {
            Some(job) => (job, Some(error.to_string())),
            None => return Err(Error::NotSettled(error)),
        },
    };
    let (outcome, report) = report(database, job, problem).await?;
    println!("{report}");
    if outcome == "completed" {
        Ok(())
    } else {
        Err(Error::NotCompleted { job, outcome })
    }
}

/// The Job an execution error reports as settled, if it does.
fn settled_job(error: &ExecutionRuntimeError) -> Option<JobId> {
    match error {
        ExecutionRuntimeError::ObservationsNotRecorded { receipt, .. }
        | ExecutionRuntimeError::ResponseNotRecorded { receipt, .. }
        | ExecutionRuntimeError::CompletionRefused { receipt, .. }
        | ExecutionRuntimeError::ToolLoopStopped { receipt, .. } => Some(receipt.job),
        _ => None,
    }
}

/// What the database recorded of the settled Job: its outcome, its result,
/// the evidence of every tool call, and each Provider request's own
/// observations. Returns the outcome with the report.
async fn report(
    database: &DatabaseHandle,
    job: JobId,
    problem: Option<String>,
) -> Result<(&'static str, String), DatabaseError> {
    let record = database.job(job).await?;
    let outcome = match &record {
        Some(record) if record.completed_at.is_some() => "completed",
        Some(record) if record.failed_at.is_some() => "failed",
        Some(record) if record.cancelled_at.is_some() => "cancelled",
        Some(record) if record.unknown_at.is_some() => "unknown",
        _ => "unsettled",
    };
    let mut report = format!("Job {job}: {outcome}\n");
    if let Some(problem) = problem {
        let _ = writeln!(report, "Stopped: {problem}");
    }
    let attachments = database
        .job_input_for_job(job)
        .await?
        .map(|input| input.attachments)
        .unwrap_or_default();
    if !attachments.is_empty() {
        let _ = writeln!(report, "Attachments:");
    }
    for attachment in attachments {
        let _ = match database.attachment(attachment).await? {
            Some(record) => writeln!(
                report,
                "  {attachment} {:?}: {}, {} bytes, sha256 {}, {:?}, {}",
                record.display_name.as_deref().unwrap_or(""),
                record.mime,
                record.byte_length,
                record.sha256,
                record.origin,
                match record.cleanup {
                    None => "available".to_owned(),
                    Some(cleanup) => format!("cleaned up {:?}", cleanup.kind),
                }
            ),
            None => writeln!(report, "  {attachment}: missing"),
        };
    }
    if let Some(output) = database.latest_output(job).await?
        && output.kind == OutputKind::Result
    {
        let _ = writeln!(
            report,
            "Result:\n{}",
            output.display_text().as_deref().unwrap_or("(none)")
        );
    }
    let _ = writeln!(report, "Tool calls:");
    for evidence in database.tool_evidence(job, None, 1_000).await? {
        let call = evidence.call;
        let _ = write!(
            report,
            "  request {}, call {} ({}) `{}`: ",
            call.request, call.index, evidence.tool_call_id, evidence.name
        );
        let _ = match &evidence.decision.fact {
            ToolDecision::Refused(refusal) => write!(report, "refused ({refusal:?})"),
            ToolDecision::AwaitingApproval(admission) | ToolDecision::AwaitingAnswer(admission) => {
                write!(
                    report,
                    "awaiting decision, arguments {}",
                    shown_arguments(&evidence.name, admission.arguments.as_str())
                )
            }
            ToolDecision::Admitted(admission) => write!(
                report,
                "admitted ({:?}, handler {}), arguments {}",
                admission.effect,
                admission.handler_version,
                shown_arguments(&evidence.name, admission.arguments.as_str())
            ),
        };
        let _ = match evidence.outcome.as_ref().map(|outcome| &outcome.fact) {
            None => writeln!(report),
            Some(ToolOutcome::Completed { result }) => writeln!(
                report,
                "; completed, {}{} result bytes",
                shown_result(&evidence.name, result.as_str()),
                result.len()
            ),
            Some(ToolOutcome::NotApplied { message }) => {
                writeln!(report, "; not applied: {message}")
            }
            Some(outcome) => writeln!(report, "; {outcome:?}"),
        };
    }
    let _ = writeln!(report, "Provider requests:");
    // Every page, so no request's usage is reduced from part of its records.
    let mut metrics: Vec<MetricsRecord> = Vec::new();
    loop {
        let after = metrics.last().map(|record| record.id);
        let page = database.job_metrics(job, after, 1_000).await?;
        if page.is_empty() {
            break;
        }
        metrics.extend(page);
    }
    let stats = database.job_availability(job, None, 1_000).await?;
    provider_requests(&mut report, &stats, &metrics);
    Ok((outcome, report))
}

/// A call's arguments as the report shows them: as recorded, except that
/// the contents a write carries are shown only by their size.
fn shown_arguments(tool: &str, arguments: &str) -> String {
    #[cfg(unix)]
    if tool == workspace_write::TOOL_NAME {
        let Ok(serde_json::Value::Object(mut fields)) = serde_json::from_str(arguments) else {
            return "(not shown)".to_owned();
        };
        for key in ["content", "expected_content", "old", "new"] {
            if let Some(field) = fields.get_mut(key) {
                *field = serde_json::Value::String(match field.as_str() {
                    Some(text) => format!("{} bytes", text.len()),
                    None => "(not shown)".to_owned(),
                });
            }
        }
        return serde_json::Value::Object(fields).to_string();
    }
    let _ = tool;
    arguments.to_owned()
}

/// What the report shows of a completed call's result besides its size: a
/// command's exit and whether its output was truncated, never the output.
fn shown_result(tool: &str, result: &str) -> String {
    #[cfg(unix)]
    if tool == command::TOOL_NAME {
        let Ok(result) = serde_json::from_str::<serde_json::Value>(result) else {
            return String::new();
        };
        let exit = match (&result["exit_code"], &result["signal"]) {
            (serde_json::Value::Number(code), _) => format!("exit code {code}"),
            (_, serde_json::Value::Number(signal)) => format!("ended by signal {signal}"),
            _ => "no exit code".to_owned(),
        };
        let truncated = if result["truncated"] == serde_json::Value::Bool(true) {
            ", output truncated"
        } else {
            ""
        };
        return format!("{exit}{truncated}, ");
    }
    let _ = (tool, result);
    String::new()
}

/// One line per Provider request: what it answered and the Token usage its
/// own annotated Metrics establish under the Token contract. Each request is
/// reduced on its own, so nothing is taken from one request for another.
fn provider_requests(
    report: &mut String,
    stats: &[AvailabilityStatRecord],
    metrics: &[MetricsRecord],
) {
    let mut requests: BTreeMap<RequestNumber, Vec<&MetricsRecord>> = BTreeMap::new();
    for record in metrics {
        requests.entry(record.request).or_default().push(record);
    }
    for stat in stats {
        let observation = &stat.observation;
        let _ = write!(
            report,
            "  request {}: status {:?}, finish {:?}",
            stat.request, observation.status_code, observation.raw_finish_reason,
        );
        let records = requests.get(&stat.request).map_or(&[][..], Vec::as_slice);
        match TokenRequestSample::from_metrics(records.iter().copied()) {
            Err(error) => {
                let _ = writeln!(report, ", token usage unavailable: {error}");
            }
            Ok(sample) => {
                let count = |count: fn(&TokenRequestSample) -> TokenCount| {
                    sample.as_ref().map_or(TokenCount::Missing, count)
                };
                let _ = write!(
                    report,
                    ", input tokens {}, output tokens {}",
                    TokenDisplay(count(|sample| sample.input)),
                    TokenDisplay(count(|sample| sample.output)),
                );
                // Detail counters only when reported; never shown as zero.
                for (name, count) in [
                    ("reasoning", count(|sample| sample.reasoning)),
                    ("cache read", count(|sample| sample.cache_read)),
                    ("cache write", count(|sample| sample.cache_write)),
                ] {
                    if count != TokenCount::Missing {
                        let _ = write!(report, ", {name} tokens {}", TokenDisplay(count));
                    }
                }
                let _ = writeln!(report);
            }
        }
    }
}

/// A counter with its certainty: a value is final or provisional, and a
/// counter without one says why rather than showing a number.
struct TokenDisplay(TokenCount);

impl std::fmt::Display for TokenDisplay {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self.0 {
            TokenCount::Final(count) => write!(f, "{count} (final)"),
            TokenCount::Provisional(count) => write!(f, "{count} (provisional)"),
            TokenCount::Missing => f.write_str("unreported"),
            TokenCount::Conflicted => f.write_str("conflicted"),
        }
    }
}
