//! `ocg workspace-read <objective>`: the executable's opt-in, one-shot Job
//! that may read files of a Workspace the operator authorized. This module
//! belongs to the executable, not to the library.
//!
//! It runs the ordinary host and submits exactly one Job through the host's
//! scheduler, offering only `workspace_read_file`, bound to the Workspace
//! named by [`WORKSPACE_ROOT_VARIABLE`]. The Provider and model are named by
//! the environment too. The credential is named only by reference: the
//! existing resolver reads it when the request is built, and it is never
//! stored, printed or logged. Once the Job settles, what the database
//! recorded of it is printed and the host stops. A signal while the Job runs
//! cancels it, and the host stops once it has settled.

use std::cell::RefCell;
use std::collections::BTreeSet;
use std::fmt::Write as _;
use std::num::NonZeroU32;
use std::path::PathBuf;
use std::rc::Rc;
use std::sync::Arc;

use ntex::util::{Either, select};

use ocg::data_directory::DataDirectory;
use ocg::database::{DatabaseError, DatabaseHandle};
use ocg::host::{self, HostContext, HostError};
use ocg::orchestration::ToolRegistryError;
use ocg::orchestration::execution_runtime::{
    ExecutionRuntimeError, ProviderExecutionPlan, ToolExecution,
};
use ocg::orchestration::workspace_read::{self, Workspace, WorkspaceError};
use ocg::persistence::{
    ExecutionConfiguration, ExecutionMode, ExecutorId, ExecutorType, JobId, JobInputDraft,
    LlmConfig, LlmParameters, LlmTransport, ModelCapability, NewExecutor, NewJob, NewLlmProvider,
    NewProfile, NewProfileConfig, OutputKind, ProfileConfig, ProfileId, ToolDecision, ToolOutcome,
};
use ocg::provider_execution::{
    CancellationToken, EnvironmentCredentialResolver, ProviderExecutionConfig,
};
use ocg::token_usage::UsageFinality;

/// The command that selects this mode, as the executable's only argument
/// before the objective.
pub const COMMAND: &str = "workspace-read";
/// The Workspace the Job may read from: an absolute path to a directory.
pub const WORKSPACE_ROOT_VARIABLE: &str = "OCG_WORKSPACE_ROOT";
/// The Provider's OpenAI-compatible endpoint, an `https://` URL.
pub const ENDPOINT_VARIABLE: &str = "OCG_PROVIDER_ENDPOINT";
pub const MODEL_VARIABLE: &str = "OCG_PROVIDER_MODEL";
/// The name of the environment variable that holds the Provider's key.
pub const CREDENTIAL_REF_VARIABLE: &str = "OCG_PROVIDER_CREDENTIAL_REF";

/// The Executor every one-shot Job runs on. It is registered once and
/// reused, as are the Provider and the LLM Profile of a configuration.
const EXECUTOR_NAME: &str = "workspace-read";

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("{0} must be set")]
    NotConfigured(&'static str),
    #[error("{ENDPOINT_VARIABLE} must be an https:// URL")]
    NotHttps,
    #[error("the variable named by {CREDENTIAL_REF_VARIABLE} is not set")]
    CredentialMissing,
    #[error("usage: ocg {COMMAND} <objective>")]
    Usage,
    #[error("workspace_read_file is not enabled: {0}")]
    Workspace(#[from] WorkspaceError),
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
    workspace_root: PathBuf,
    endpoint: String,
    model: String,
    credential_ref: String,
    objective: String,
}

impl Config {
    /// Reads the configuration from the environment, with `objective` given
    /// on the command line. The credential itself is only checked to exist.
    pub fn from_environment(objective: Option<String>) -> Result<Self, Error> {
        let objective = objective
            .filter(|objective| !objective.trim().is_empty())
            .ok_or(Error::Usage)?;
        let variable = |name: &'static str| {
            std::env::var(name)
                .ok()
                .filter(|value| !value.trim().is_empty())
                .ok_or(Error::NotConfigured(name))
        };
        let workspace_root = std::env::var_os(WORKSPACE_ROOT_VARIABLE)
            .filter(|root| !root.is_empty())
            .map(PathBuf::from)
            .ok_or(Error::NotConfigured(WORKSPACE_ROOT_VARIABLE))?;
        let endpoint = variable(ENDPOINT_VARIABLE)?;
        if !endpoint.starts_with("https://") {
            return Err(Error::NotHttps);
        }
        let model = variable(MODEL_VARIABLE)?;
        let credential_ref = variable(CREDENTIAL_REF_VARIABLE)?;
        if std::env::var_os(&credential_ref).is_none_or(|credential| credential.is_empty()) {
            return Err(Error::CredentialMissing);
        }
        Ok(Self {
            workspace_root,
            endpoint,
            model,
            credential_ref,
            objective,
        })
    }
}

/// Authorizes the Workspace, then runs the host until the one Job has
/// settled and been reported. Nothing is recorded, and the host does not
/// start, unless the Workspace is authorized.
pub fn run(data_directory: &DataDirectory, config: Config) -> Result<(), Error> {
    let workspace = Workspace::authorize(&config.workspace_root, data_directory)?;
    eprintln!(
        "OCG Workspace authorized for {}: {}",
        workspace_read::TOOL_NAME,
        workspace.root().display()
    );
    let tools = workspace_read::read_file_tools(workspace)?;
    let settled = Rc::new(RefCell::new(None));
    let slot = settled.clone();
    let hosted = host::run_hosted(data_directory, true, move |context| async move {
        *slot.borrow_mut() = Some(submit(&context, config, tools).await);
    });
    let settled = settled.borrow_mut().take();
    match (settled, hosted) {
        (Some(Err(error)), _) => Err(error),
        (_, Err(error)) => Err(error.into()),
        (None, Ok(())) => Err(Error::NotRun),
        (Some(Ok(())), Ok(())) => Ok(()),
    }
}

/// Submits the Job, waits for it to settle, and prints what was recorded.
async fn submit(context: &HostContext, config: Config, tools: ToolExecution) -> Result<(), Error> {
    // Registered first, so a signal while the Job is prepared cancels it too.
    let signal = ntex::rt::signals::signal();
    let database = context.database();
    let (executor, llm_profile) = targets(database, &config).await?;
    let cancellation = CancellationToken::default();
    let plan = ProviderExecutionPlan {
        executor_id: executor,
        input: JobInputDraft {
            objective: Some(config.objective),
            ..JobInputDraft::default()
        },
        job: NewJob {
            name: Some(format!("{COMMAND} one-shot")),
            execution_mode: ExecutionMode::Once,
            eligible_at: None,
            scheduled_at: None,
            next_due_at: None,
            next_iteration_no: None,
            iteration_no: None,
            parent: database.genesis(),
            projects: Vec::new(),
            dependencies: Vec::new(),
            configuration: Some(ExecutionConfiguration {
                agent_profile: None,
                llm_profile,
            }),
            assigned_executor: None,
            previous: None,
            replaces: None,
        },
        credential_resolver: Arc::new(EnvironmentCredentialResolver),
        provider_config: ProviderExecutionConfig {
            cancellation: cancellation.clone(),
            ..ProviderExecutionConfig::default()
        },
        tools: Some(tools),
    };
    let mut scheduled = std::pin::pin!(context.scheduler().schedule_provider(plan));
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
        | ExecutionRuntimeError::CompletionRefused { receipt, .. }
        | ExecutionRuntimeError::ToolLoopStopped { receipt, .. }
        | ExecutionRuntimeError::StopNotConfirmed { receipt, .. } => Some(receipt.job),
        _ => None,
    }
}

/// The Executor and LLM Profile for `config`, reusing those an earlier run
/// registered for the same endpoint, credential reference and model.
async fn targets(
    database: &DatabaseHandle,
    config: &Config,
) -> Result<(ExecutorId, ProfileId), DatabaseError> {
    let transport = LlmTransport::OpenAiChatCompletions;
    let existing = database
        .executors(None, 1_000)
        .await?
        .into_iter()
        .find(|executor| {
            executor.name == EXECUTOR_NAME
                && executor.executor_type == ExecutorType::Llm
                && executor.llm_transports.contains(&transport)
        });
    let executor = match existing {
        Some(executor) => executor.id,
        None => {
            database
                .create_executor(NewExecutor {
                    name: EXECUTOR_NAME.to_owned(),
                    executor_type: ExecutorType::Llm,
                    concurrency_limit: NonZeroU32::MIN,
                    llm_transports: BTreeSet::from([transport]),
                })
                .await?
        }
    };
    let existing = database
        .llm_providers(None, 1_000)
        .await?
        .into_iter()
        .find(|provider| {
            provider.transport == transport
                && provider.endpoint.as_deref() == Some(config.endpoint.as_str())
                && provider.credential_ref.as_deref() == Some(config.credential_ref.as_str())
        });
    let provider = match existing {
        Some(provider) => provider.id,
        None => {
            database
                .create_llm_provider(NewLlmProvider {
                    name: EXECUTOR_NAME.to_owned(),
                    transport,
                    endpoint: Some(config.endpoint.clone()),
                    credential_ref: Some(config.credential_ref.clone()),
                })
                .await?
        }
    };
    let llm = LlmConfig {
        model: config.model.clone(),
        parameters: LlmParameters::default(),
        declared_capabilities: BTreeSet::from([ModelCapability::Chat, ModelCapability::Tools]),
    };
    for profile in database.provider_profiles(provider).await? {
        if let Some(record) = database.profile(profile).await?
            && record.config == ProfileConfig::Llm(llm.clone())
        {
            return Ok((executor, profile));
        }
    }
    let profile = database
        .create_profile(NewProfile {
            name: config.model.clone(),
            config: NewProfileConfig::Llm {
                provider,
                config: llm,
            },
        })
        .await?;
    Ok((executor, profile))
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
    if let Some(output) = database.latest_output(job).await?
        && output.kind == OutputKind::Result
    {
        let _ = writeln!(
            report,
            "Result:\n{}",
            output.summary.as_deref().unwrap_or("(none)")
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
            ToolDecision::Admitted(admission) => write!(
                report,
                "admitted ({:?}, handler {}), arguments {}",
                admission.effect,
                admission.handler_version,
                admission.arguments.as_str()
            ),
        };
        let _ = match evidence.outcome.as_ref().map(|outcome| &outcome.fact) {
            None => writeln!(report),
            Some(ToolOutcome::Completed { result }) => {
                writeln!(report, "; completed, {} result bytes", result.len())
            }
            Some(ToolOutcome::NotApplied { message }) => {
                writeln!(report, "; not applied: {message}")
            }
            Some(outcome) => writeln!(report, "; {outcome:?}"),
        };
    }
    let _ = writeln!(report, "Provider requests:");
    let metrics = database.job_metrics(job, None, 1_000).await?;
    for stat in database.job_availability(job, None, 1_000).await? {
        let usage = metrics
            .iter()
            .rev()
            .find(|record| {
                record.request == stat.request
                    && record
                        .token
                        .is_some_and(|token| token.finality == UsageFinality::Final)
            })
            .map(|record| &record.measurements.usage);
        let observation = &stat.observation;
        let _ = writeln!(
            report,
            "  request {}: status {:?}, finish {:?}, input tokens {:?}, output tokens {:?}",
            stat.request,
            observation.status_code,
            observation.raw_finish_reason,
            usage.and_then(|usage| usage.input_tokens),
            usage.and_then(|usage| usage.output_tokens),
        );
    }
    Ok((outcome, report))
}
