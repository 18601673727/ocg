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
use std::collections::{BTreeMap, BTreeSet};
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
    AvailabilityStatRecord, ExecutionConfiguration, ExecutionMode, ExecutorId, ExecutorType, JobId,
    JobInputDraft, LlmConfig, LlmParameters, LlmTransport, MetricsRecord, ModelCapability,
    NewExecutor, NewJob, NewLlmProvider, NewProfile, NewProfileConfig, OutputKind, ProfileConfig,
    ProfileId, RequestNumber, ToolDecision, ToolOutcome,
};
use ocg::provider_execution::{
    CancellationToken, EnvironmentCredentialResolver, ProviderExecutionConfig,
};
use ocg::token_usage::{TokenCount, TokenRequestSample};

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

#[cfg(test)]
mod tests {
    use ocg::openai_compatible::NormalizedUsage;
    use ocg::persistence::{
        AvailabilityObservation, AvailabilityStatId, FrozenLlmConfig, LlmProviderId, LlmTarget,
        Measurements, MetricsId, ProviderSnapshot, Timestamp,
    };
    use ocg::token_usage::{RequestEligibility, TokenAnnotation, UsageFinality};

    use super::*;

    const CUMULATIVE: Option<TokenAnnotation> =
        Some(TokenAnnotation::eligible(UsageFinality::Cumulative));
    const FINAL: Option<TokenAnnotation> = Some(TokenAnnotation::eligible(UsageFinality::Final));

    fn request(number: u32) -> RequestNumber {
        RequestNumber::new(number).unwrap()
    }

    fn target() -> LlmTarget {
        LlmTarget {
            llm_profile: ProfileId::new(1),
            frozen: FrozenLlmConfig {
                config: LlmConfig {
                    model: "model".to_owned(),
                    parameters: LlmParameters::default(),
                    declared_capabilities: BTreeSet::from([ModelCapability::Chat]),
                },
                provider: ProviderSnapshot {
                    id: LlmProviderId::new(1),
                    name: "provider".to_owned(),
                    transport: LlmTransport::OpenAiChatCompletions,
                    endpoint: None,
                    credential_ref: None,
                },
            },
        }
    }

    /// A Metrics record of `request` with input, output and reasoning counts.
    fn metrics(
        id: u64,
        request: RequestNumber,
        [input, output, reasoning]: [Option<u32>; 3],
        token: Option<TokenAnnotation>,
    ) -> MetricsRecord {
        MetricsRecord {
            id: MetricsId::new(id),
            job: JobId::new(3),
            request,
            target: Some(target()),
            observed_at: Timestamp::from_unix_micros(i64::try_from(id).unwrap()),
            measurements: Measurements {
                ttfb_ms: None,
                reasoning_ttft_ms: None,
                output_ttft_ms: None,
                request_finished_in_ms: None,
                usage: NormalizedUsage {
                    input_tokens: input,
                    output_tokens: output,
                    reasoning_tokens: reasoning,
                    ..NormalizedUsage::default()
                },
            },
            token,
            recorded_at: Timestamp::from_unix_micros(i64::try_from(id).unwrap()),
        }
    }

    fn stat(request: RequestNumber, finish: &str) -> AvailabilityStatRecord {
        AvailabilityStatRecord {
            id: AvailabilityStatId::new(u64::from(request.get())),
            job: JobId::new(3),
            request,
            target: target(),
            observed_at: Timestamp::from_unix_micros(0),
            observation: AvailabilityObservation {
                status_code: Some(200),
                finish_reason: None,
                raw_finish_reason: Some(finish.to_owned()),
                failure: None,
            },
            recorded_at: Timestamp::from_unix_micros(0),
        }
    }

    fn lines(stats: &[AvailabilityStatRecord], metrics: &[MetricsRecord]) -> String {
        let mut report = String::new();
        provider_requests(&mut report, stats, metrics);
        report
    }

    #[test]
    fn cumulative_usage_is_shown_as_provisional() {
        let report = lines(
            &[stat(request(1), "tool_calls")],
            &[
                metrics(1, request(1), [None; 3], CUMULATIVE),
                metrics(2, request(1), [Some(112), Some(20), Some(0)], CUMULATIVE),
            ],
        );
        assert_eq!(
            report,
            "  request request 1: status Some(200), finish Some(\"tool_calls\"), \
             input tokens 112 (provisional), output tokens 20 (provisional), \
             reasoning tokens 0 (provisional)\n"
        );
    }

    #[test]
    fn final_usage_is_shown_as_final_and_explicit_zero_stays_zero() {
        let report = lines(
            &[stat(request(1), "stop")],
            &[metrics(1, request(1), [Some(0), Some(54), Some(0)], FINAL)],
        );
        assert_eq!(
            report,
            "  request request 1: status Some(200), finish Some(\"stop\"), \
             input tokens 0 (final), output tokens 54 (final), reasoning tokens 0 (final)\n"
        );
    }

    #[test]
    fn cumulative_observations_take_their_maximum_not_their_sum() {
        let report = lines(
            &[stat(request(1), "stop")],
            &[
                metrics(2, request(1), [Some(112), Some(20), None], CUMULATIVE),
                metrics(1, request(1), [Some(100), Some(7), None], CUMULATIVE),
            ],
        );
        assert!(
            report.contains("input tokens 112 (provisional), output tokens 20 (provisional)\n"),
            "{report}"
        );
    }

    #[test]
    fn a_final_observation_wins_over_cumulative_values() {
        let report = lines(
            &[stat(request(1), "stop")],
            &[
                metrics(1, request(1), [Some(112), Some(30), None], FINAL),
                metrics(2, request(1), [Some(150), Some(40), None], CUMULATIVE),
            ],
        );
        assert!(
            report.contains("input tokens 112 (final), output tokens 30 (final)\n"),
            "{report}"
        );
    }

    #[test]
    fn missing_counters_stay_unreported() {
        let report = lines(
            &[stat(request(1), "stop"), stat(request(2), "stop")],
            &[metrics(1, request(1), [Some(112), None, None], CUMULATIVE)],
        );
        assert_eq!(
            report,
            "  request request 1: status Some(200), finish Some(\"stop\"), \
             input tokens 112 (provisional), output tokens unreported\n  \
             request request 2: status Some(200), finish Some(\"stop\"), \
             input tokens unreported, output tokens unreported\n"
        );
    }

    #[test]
    fn conflicting_finals_are_shown_as_conflicted() {
        let report = lines(
            &[stat(request(1), "stop")],
            &[
                metrics(1, request(1), [Some(112), Some(20), Some(3)], FINAL),
                metrics(2, request(1), [Some(113), Some(20), Some(4)], FINAL),
            ],
        );
        assert!(
            report.contains(
                "input tokens conflicted, output tokens 20 (final), reasoning tokens conflicted\n"
            ),
            "{report}"
        );
    }

    #[test]
    fn requests_of_one_job_are_reduced_independently() {
        let report = lines(
            &[stat(request(1), "tool_calls"), stat(request(2), "stop")],
            &[
                metrics(4, request(2), [Some(223), Some(54), Some(0)], CUMULATIVE),
                metrics(1, request(1), [Some(112), Some(20), Some(0)], CUMULATIVE),
                metrics(3, request(2), [Some(200), None, None], CUMULATIVE),
                metrics(2, request(1), [Some(112), Some(20), Some(0)], FINAL),
            ],
        );
        assert_eq!(
            report,
            "  request request 1: status Some(200), finish Some(\"tool_calls\"), \
             input tokens 112 (final), output tokens 20 (final), reasoning tokens 0 (final)\n  \
             request request 2: status Some(200), finish Some(\"stop\"), \
             input tokens 223 (provisional), output tokens 54 (provisional), \
             reasoning tokens 0 (provisional)\n"
        );
    }

    #[test]
    fn unannotated_metrics_are_not_token_evidence() {
        let report = lines(
            &[stat(request(1), "stop")],
            &[
                metrics(1, request(1), [Some(500), Some(500), Some(500)], None),
                metrics(2, request(1), [Some(112), None, None], CUMULATIVE),
            ],
        );
        assert!(
            report.ends_with("input tokens 112 (provisional), output tokens unreported\n"),
            "{report}"
        );
    }

    #[test]
    fn records_that_disagree_on_eligibility_report_no_usage() {
        let excluded = TokenAnnotation::excluded();
        assert_eq!(excluded.eligibility, RequestEligibility::Excluded);
        let report = lines(
            &[stat(request(1), "stop")],
            &[
                metrics(1, request(1), [None; 3], Some(excluded)),
                metrics(2, request(1), [Some(112), Some(20), None], FINAL),
            ],
        );
        assert!(
            report.starts_with(
                "  request request 1: status Some(200), finish Some(\"stop\"), \
                 token usage unavailable: "
            ),
            "{report}"
        );
        assert!(!report.contains("112"), "{report}");
    }
}
