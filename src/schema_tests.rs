//! Contract tests for the persisted schema, driven through the database
//! worker exactly as callers use it: commands on the write thread, reads on
//! the read pool, and close/reopen of the same file.

use std::num::NonZeroU32;

use crate::data_directory::DataDirectory;
use crate::database::{DatabaseError, DatabaseHandle, DatabaseWorker};
use std::collections::BTreeSet;

use serde_json::json;

use crate::openai_compatible::{ChatFinishReason, NormalizedUsage, ProviderFailure};
use crate::orchestration::execution_runtime::{
    ExecutionPlan, ExecutionRuntime, ExecutionRuntimeError, settle_provider_error,
};
use crate::persistence::{
    AgentBehavior, AgentConfig, AvailabilityClassification, AvailabilityObservation,
    AvailabilityRequestSample, AvailabilityStatRecord, ConfigBody, ConfigId, ConfigKind,
    ConnectionConfig, DependencyCondition, ExecutionConfiguration, ExecutorId, FinalResult,
    IntermediateOutputKind, JobFinalization, JobId, JobInputDraft, JobOutcome, JobRecord, JobRetry,
    LlmParameters, LlmProviderId, LlmTransport, Measurements, MetricsRecord, MixedRequests,
    ModelCapability, ModelConfig, NewAvailabilityStat, NewConfig, NewExecutor, NewJob,
    NewJobOutput, NewMetrics, NewProject, ObservationFilter, OutputKind, ProfileId, ProjectId,
    RecordRef, RejectionCause, RequestNumber, TimeRange, Timestamp, UnavailableCause, Violation,
};
use crate::provider_execution::{ProviderExecutionError, ResponseFacts};
use crate::test_support::{TestRoot, data_directory as test_data_directory, poll_once, wait};

const SUITE: &str = "ocg-schema";
/// The transport of the only LLM adapter OCG implements.
const OPENAI: LlmTransport = LlmTransport::OpenAiChatCompletions;

fn data_directory(name: &str) -> (TestRoot, DataDirectory) {
    test_data_directory(SUITE, name)
}

fn new_job(handle: &DatabaseHandle, name: &str, parent: JobId, projects: Vec<ProjectId>) -> NewJob {
    NewJob {
        name: Some(name.into()),
        eligible_at: None,
        parent,
        projects,
        dependencies: Vec::new(),
        input: JobInputDraft {
            objective: Some(name.into()),
            ..JobInputDraft::default()
        },
        configuration: Some(crate::test_support::execution_configuration(handle)),
        previous: None,
    }
}

fn create_job(handle: &DatabaseHandle, job: NewJob) -> JobId {
    wait(handle.create_job(job)).expect("create job")
}

#[test]
fn provider_progress_skips_empty_text_and_preserves_whitespace() {
    use crate::provider_execution::ProviderOutputSink;

    let (_root, directory) = data_directory("blank-progress");
    let worker = DatabaseWorker::start(&directory).expect("start");
    let handle = worker.handle();
    let job = create_job(
        &handle,
        new_job(&handle, "progress", handle.genesis(), vec![]),
    );
    wait(handle.claim_fixture(job, test_executor(&handle))).expect("claim");
    let mut sink = crate::orchestration::execution_runtime::output_sink(handle.clone(), job);
    for text in ["", " \t\n", "\u{2003}", " answer \n"] {
        wait(sink.append(text)).expect("append");
    }
    let outputs = wait(handle.job_outputs(job, None, 10)).expect("outputs");
    assert_eq!(outputs.len(), 3);
    for (output, (seq, text)) in outputs.iter().zip([(1, " \t\n"), (2, "\u{2003}"), (3, " answer \n")]) {
        assert_eq!(output.seq, seq);
        assert_eq!(output.summary.as_deref(), Some(text));
    }
    worker.shutdown().expect("shutdown");
}

/// The Pool that runs the plain Jobs of these tests: it offers no work to the
/// claim batch.
fn test_executor(handle: &DatabaseHandle) -> ExecutorId {
    wait(handle.executors(None, 100))
        .expect("read Executors")
        .into_iter()
        .find(|executor| executor.name == "test-executor")
        .map(|executor| executor.id)
        .unwrap_or_else(|| {
            wait(handle.create_executor(NewExecutor {
                name: "test-executor".into(),
                concurrency_limit: NonZeroU32::new(16).expect("fixture capacity"),
                llm_transports: BTreeSet::from([OPENAI]),
            }))
            .expect("register the test Executor")
        })
}

/// Creates a Job and starts it as this process's own execution, in one
/// transaction.
fn local_job(handle: &DatabaseHandle, job: NewJob) -> JobId {
    wait(handle.create_claimed_fixture(job, test_executor(handle)))
        .expect("create and start the Job")
}

fn violation<T: std::fmt::Debug>(result: Result<T, DatabaseError>) -> Violation {
    match result {
        Err(DatabaseError::Rejected(violation)) => violation,
        other => panic!("expected a rule violation, got {other:?}"),
    }
}

fn protection_reason(directory: &DataDirectory) -> crate::disk_protection::ProtectionReason {
    use crate::disk_protection::{ProtectedArea, ProtectionReason, SpaceShortage};
    ProtectionReason::InsufficientSpace(SpaceShortage {
        area: ProtectedArea::Database,
        path: directory.database_path().to_path_buf(),
        available_bytes: 0,
        expected_write_bytes: 0,
        threshold_bytes: 1,
    })
}

fn result(summary: &str) -> FinalResult {
    FinalResult {
        summary: Some(summary.into()),
        code: None,
        goal_reached: None,
        value: None,
    }
}

fn project(handle: &DatabaseHandle, name: &str) -> ProjectId {
    wait(handle.create_project(NewProject {
        name: name.into(),
        description: None,
    }))
    .expect("create project")
}

fn provider(handle: &DatabaseHandle, name: &str) -> LlmProviderId {
    wait(handle.create_config(NewConfig {
        name: name.into(),
        body: ConfigBody::Connection(ConnectionConfig {
            protocol: OPENAI,
            endpoint: Some(format!("https://{name}.invalid/v1")),
            credential_ref: Some(format!("{}_KEY", name.to_uppercase())),
        }),
    }))
    .expect("create Connection")
    .into()
}

/// What a Model config holds, besides its Connection.
#[derive(Clone)]
struct LlmConfig {
    model: String,
    context_window: Option<u32>,
    parameters: LlmParameters,
    declared_capabilities: BTreeSet<ModelCapability>,
}

/// A Connection without a credential, at `endpoint`.
fn connection(handle: &DatabaseHandle, name: &str, endpoint: Option<String>) -> LlmProviderId {
    wait(handle.create_config(NewConfig {
        name: name.into(),
        body: ConfigBody::Connection(ConnectionConfig {
            protocol: OPENAI,
            endpoint,
            credential_ref: None,
        }),
    }))
    .expect("create Connection")
    .into()
}

fn llm_config(model: &str, declared: &[ModelCapability]) -> LlmConfig {
    LlmConfig {
        model: model.into(),
        context_window: None,
        parameters: LlmParameters {
            temperature: Some(0.2),
            max_tokens: Some(256),
            ..LlmParameters::default()
        },
        declared_capabilities: declared.iter().copied().collect(),
    }
}

fn llm_profile(handle: &DatabaseHandle, provider: LlmProviderId, config: LlmConfig) -> ProfileId {
    wait(handle.create_config(NewConfig {
        name: config.model.clone(),
        body: ConfigBody::Model(ModelConfig {
            connection_id: provider.into(),
            model: config.model,
            context_window: config.context_window,
            parameters: config.parameters,
            declared_capabilities: config.declared_capabilities,
        }),
    }))
    .expect("create Model config")
    .into()
}

fn agent_profile(
    handle: &DatabaseHandle,
    instructions: &str,
    llm_targets: Vec<ProfileId>,
) -> ProfileId {
    wait(handle.create_config(NewConfig {
        name: "assistant".into(),
        body: ConfigBody::Agent(AgentConfig {
            instructions: Some(instructions.into()),
            behavior: AgentBehavior::Chat,
            models: llm_targets.into_iter().map(Into::into).collect(),
            ..Default::default()
        }),
    }))
    .expect("create Agent config")
    .into()
}

fn provider_failure(message: &str, status_code: Option<u16>, retryable: bool) -> ProviderFailure {
    ProviderFailure {
        message: message.into(),
        status_code,
        provider_code: status_code.map(|_| "invalid_api_key".into()),
        response_body: None,
        request_id: Some("request-1".into()),
        retry_after_ms: None,
        retryable,
    }
}

fn at(micros: i64) -> Timestamp {
    Timestamp::from_unix_micros(micros)
}

fn observed_between(from: i64, until: i64) -> ObservationFilter {
    ObservationFilter {
        observed: TimeRange {
            from: at(from),
            until: at(until),
        },
        llm_profile: None,
        provider: None,
        model: None,
    }
}

fn all_time() -> ObservationFilter {
    observed_between(i64::MIN, i64::MAX)
}

fn of_profile(llm_profile: ProfileId) -> ObservationFilter {
    ObservationFilter {
        llm_profile: Some(llm_profile),
        ..all_time()
    }
}

fn response(status_code: u16) -> AvailabilityObservation {
    AvailabilityObservation {
        status_code: Some(status_code),
        finish_reason: Some(ChatFinishReason::Stop),
        raw_finish_reason: Some("stop".into()),
        failure: None,
    }
}

fn failure(failure: ProviderFailure) -> AvailabilityObservation {
    AvailabilityObservation {
        status_code: failure.status_code,
        finish_reason: None,
        raw_finish_reason: None,
        failure: Some(failure),
    }
}

fn assert_relationships_consistent(directory: &DataDirectory) {
    let opened = crate::storage::open(directory.database_path())
        .unwrap_or_else(|_| panic!("open the closed database file"));
    let problems =
        crate::storage::relationship_inconsistencies(&opened.database).expect("scan relationships");
    assert!(
        problems.is_empty(),
        "relationship inconsistencies: {problems:#?}"
    );
}

fn executor(
    handle: &DatabaseHandle,
    name: &str,
    llm_transports: &[LlmTransport],
) -> Result<ExecutorId, DatabaseError> {
    wait(handle.create_executor(NewExecutor {
        name: name.into(),
        concurrency_limit: NonZeroU32::MIN,
        llm_transports: llm_transports.iter().copied().collect(),
    }))
}

/// One Provider, two LLM Profiles and an Agent Profile that targets only the first.
struct Configured {
    targeted: ProfileId,
    agent: ProfileId,
}

fn configured(handle: &DatabaseHandle) -> Configured {
    let provider = provider(handle, "first");
    let targeted = llm_profile(
        handle,
        provider,
        llm_config("model-a", &[ModelCapability::Chat, ModelCapability::Tools]),
    );
    llm_profile(
        handle,
        provider,
        llm_config("model-b", &[ModelCapability::Chat]),
    );
    let agent = agent_profile(handle, "Answer briefly.", vec![targeted]);
    Configured { targeted, agent }
}

fn configuration(
    agent_profile: ProfileId,
    llm_profile: ProfileId,
) -> Option<ExecutionConfiguration> {
    Some(ExecutionConfiguration {
        agent_profile: Some(agent_profile),
        llm_profile,
    })
}

/// Work that runs an LLM directly, without an Agent Profile.
fn llm_only(llm_profile: ProfileId) -> Option<ExecutionConfiguration> {
    Some(ExecutionConfiguration {
        agent_profile: None,
        llm_profile,
    })
}

/// Polls a command once, which admits it to the write worker's queue, and
/// returns a waiter for its result. Commands run in admission order.
fn admit<F: std::future::Future>(future: F) -> impl FnOnce() -> F::Output {
    let mut future = Box::pin(future);
    let ready = poll_once(future.as_mut());
    move || match ready {
        Some(output) => output,
        None => wait(future),
    }
}

fn configured_child(handle: &DatabaseHandle, agent: ProfileId, llm: ProfileId) -> JobId {
    create_job(
        handle,
        NewJob {
            configuration: configuration(agent, llm),
            ..new_job(handle, "model request", handle.genesis(), Vec::new())
        },
    )
}

/// The Jobs submitted with `profile` that have not stopped: those that still
/// have, or may have, work under way.
fn blocking(handle: &DatabaseHandle, profile: ProfileId) -> Vec<JobId> {
    let mut jobs: Vec<JobId> = wait(handle.jobs(None, 1_000))
        .expect("read Jobs")
        .into_iter()
        .filter(|job| {
            job.configuration
                .is_some_and(|configuration| configuration.llm_profile == profile)
                && !job.locally_stopped()
        })
        .map(|job| job.id)
        .collect();
    jobs.sort();
    jobs
}

fn llm_setup(handle: &DatabaseHandle, model: &str) -> (LlmProviderId, ProfileId) {
    let provider = provider(handle, "provider");
    (
        provider,
        llm_profile(
            handle,
            provider,
            llm_config(model, &[ModelCapability::Chat]),
        ),
    )
}

/// A configured Job that runs `llm` directly, with no Agent Profile.
fn llm_job(handle: &DatabaseHandle, llm: ProfileId) -> JobId {
    create_job(
        handle,
        NewJob {
            configuration: llm_only(llm),
            ..new_job(handle, "request", handle.genesis(), Vec::new())
        },
    )
}

#[test]
fn nodes_and_relationships_round_trip_through_reopening() {
    let (_root, directory) = data_directory("round-trip");
    let worker = DatabaseWorker::start(&directory).expect("start database");
    let handle = worker.handle();
    let genesis = handle.genesis();
    let alpha = project(&handle, "alpha");
    let beta = project(&handle, "beta");
    let first_provider = provider(&handle, "first");
    let second_provider = provider(&handle, "second");
    let llm = llm_profile(
        &handle,
        first_provider,
        llm_config("model-a", &[ModelCapability::Chat, ModelCapability::Tools]),
    );
    let assistant = agent_profile(&handle, "Answer briefly.", vec![llm]);
    let executor = wait(handle.create_executor(NewExecutor {
        name: "worker".into(),
        concurrency_limit: NonZeroU32::MIN,
        llm_transports: BTreeSet::from([OPENAI]),
    }))
    .expect("create Executor");
    let parent = create_job(
        &handle,
        NewJob {
            ..new_job(&handle, "parent", genesis, vec![alpha, beta])
        },
    );
    let child = create_job(&handle, new_job(&handle, "child", parent, Vec::new()));
    let dependent = create_job(
        &handle,
        NewJob {
            dependencies: vec![(parent, DependencyCondition::Settled)],
            ..new_job(&handle, "dependent", genesis, vec![beta])
        },
    );
    wait(handle.append_job_output(
        parent,
        NewJobOutput {
            kind: IntermediateOutputKind::Progress,
            summary: Some("halfway".into()),
            code: None,
            goal_reached: None,
            value: None,
        },
    ))
    .expect("append output");
    let check = create_job(
        &handle,
        NewJob {
            configuration: llm_only(llm),
            ..new_job(&handle, "chat check", genesis, Vec::new())
        },
    );
    wait(handle.claim_fixture(check, executor)).expect("start check");
    wait(handle.finalize_job(
        check,
        JobFinalization::Completed(FinalResult {
            value: Some(json!({"capability": "chat", "observed": "supported"})),
            ..result("chat answered")
        }),
    ))
    .expect("finalize check");
    let measured = wait(handle.record_metrics(NewMetrics {
        token: None,
        job: check,
        request: RequestNumber::FIRST,
        observed_at: at(1_000),
        measurements: Measurements {
            ttfb_ms: Some(120),
            usage: NormalizedUsage {
                input_tokens: Some(12),
                output_tokens: Some(3),
                raw: Some(json!({"prompt_tokens": 12, "completion_tokens": 3})),
                ..NormalizedUsage::default()
            },
            ..Measurements::default()
        },
    }))
    .expect("record Metrics");
    let observed = wait(handle.record_availability_stat(NewAvailabilityStat {
        job: check,
        request: RequestNumber::FIRST,
        observed_at: at(1_000),
        observation: response(200),
    }))
    .expect("record AvailabilityStat");

    let read_all = |handle: &DatabaseHandle| {
        (
            wait(handle.jobs(None, 100)).expect("read jobs"),
            wait(handle.job_outputs(parent, None, 100)).expect("read outputs"),
            wait(handle.projects(None, 100)).expect("read projects"),
            wait(handle.project_jobs(beta, None, 100)).expect("read Project Jobs"),
            wait(handle.configs(None, None, 100)).expect("read configs"),
            wait(handle.job_metrics(check, None, 100)).expect("read Metrics"),
            wait(handle.children(parent, None, 100)).expect("read children"),
            wait(handle.job_availability(check, None, 100)).expect("read AvailabilityStats"),
            wait(handle.job_outputs(check, None, 100)).expect("read check outputs"),
        )
    };
    for job in [dependent, child, parent] {
        wait(handle.finalize_job(job, JobFinalization::Cancelled(result("finished fixture"))))
            .expect("settle before the round trip");
    }
    let before = read_all(&handle);
    worker.shutdown().expect("shutdown");
    assert_relationships_consistent(&directory);

    let worker = DatabaseWorker::start(&directory).expect("reopen database");
    let handle = worker.handle();
    assert_eq!(handle.genesis(), genesis);
    let after = read_all(&handle);
    assert_eq!(
        after, before,
        "every node and relationship reads back the same after reopening"
    );

    let (
        jobs,
        outputs,
        projects,
        beta_jobs,
        configs,
        metrics,
        children,
        availability,
        check_outputs,
    ) = after;
    assert_eq!(jobs.len(), 4, "the four created Jobs");
    let parent_record = jobs
        .iter()
        .find(|job| job.id == parent)
        .expect("parent persisted");
    assert_eq!(
        (parent_record.parent, parent_record.projects.clone()),
        (genesis, vec![alpha, beta])
    );
    assert_eq!(parent_record.assigned_executor, None, "nothing claimed it");
    assert_eq!(outputs.len(), 2);
    assert_eq!(projects.len(), 2);
    assert_eq!(beta_jobs, [parent, dependent]);
    assert_eq!(
        configs.len(),
        7,
        "three Connections, two Models, an Agent and the Pool of the Executor"
    );
    for kind in [
        ConfigKind::Connection,
        ConfigKind::Model,
        ConfigKind::Agent,
        ConfigKind::Pool,
    ] {
        assert!(
            configs.iter().any(|config| config.kind == kind),
            "a {kind:?} config is in the config table"
        );
    }
    assert!(
        configs.iter().any(
            |config| config.id == ConfigId::from(assistant) && config.kind == ConfigKind::Agent
        )
    );
    assert!(
        configs
            .iter()
            .any(|config| config.id == ConfigId::from(executor))
    );
    assert!(
        configs
            .iter()
            .any(|config| config.id == ConfigId::from(second_provider))
    );
    assert_eq!(
        metrics.iter().map(|metrics| metrics.id).collect::<Vec<_>>(),
        [measured]
    );
    assert_eq!(metrics[0].measurements.usage.input_tokens, Some(12));
    assert_eq!(
        availability.iter().map(|stat| stat.id).collect::<Vec<_>>(),
        [observed]
    );
    assert_eq!(availability[0].target.llm_profile, llm);
    assert_eq!(
        check_outputs[0].value,
        Some(json!({"capability": "chat", "observed": "supported"}))
    );
    assert_eq!(children, [child]);
    worker.shutdown().expect("shutdown");
}

#[test]
fn job_creation_refuses_missing_endpoints_without_partial_writes() {
    let (_root, directory) = data_directory("endpoints");
    let worker = DatabaseWorker::start(&directory).expect("start database");
    let handle = worker.handle();
    let genesis = handle.genesis();
    let existing = project(&handle, "existing");
    let jobs_before = wait(handle.jobs(None, 100)).expect("read jobs");

    let missing_job = JobId::new(9_999);
    let missing_project = ProjectId::new(9_999);
    let attempts = [
        (
            new_job(&handle, "orphan", missing_job, Vec::new()),
            Violation::NotFound(RecordRef::Job(missing_job)),
        ),
        (
            new_job(&handle, "lost", genesis, vec![existing, missing_project]),
            Violation::NotFound(RecordRef::Project(missing_project)),
        ),
        (
            NewJob {
                dependencies: vec![(missing_job, DependencyCondition::Success)],
                ..new_job(&handle, "waits", genesis, Vec::new())
            },
            Violation::NotFound(RecordRef::Job(missing_job)),
        ),
        (
            new_job(&handle, "twice", genesis, vec![existing, existing]),
            Violation::DuplicateReference(RecordRef::Project(existing)),
        ),
    ];
    for (job, expected) in attempts {
        assert_eq!(violation(wait(handle.create_job(job))), expected);
    }

    assert_eq!(
        wait(handle.jobs(None, 100)).expect("read jobs"),
        jobs_before,
        "no Job was committed"
    );
    assert!(
        wait(handle.project_jobs(existing, None, 100))
            .expect("read Project Jobs")
            .is_empty()
    );
    worker.shutdown().expect("shutdown");
}

#[test]
fn project_sets_are_independent_of_the_parent_tree() {
    let (_root, directory) = data_directory("projects");
    let worker = DatabaseWorker::start(&directory).expect("start database");
    let handle = worker.handle();
    let genesis = handle.genesis();
    let (alpha, beta, gamma) = (
        project(&handle, "alpha"),
        project(&handle, "beta"),
        project(&handle, "gamma"),
    );

    let parent = create_job(
        &handle,
        new_job(&handle, "parent", genesis, vec![alpha, beta]),
    );
    let unassociated = create_job(
        &handle,
        new_job(&handle, "unassociated", parent, Vec::new()),
    );
    let elsewhere = create_job(&handle, new_job(&handle, "elsewhere", parent, vec![gamma]));
    let projects_of = |job| {
        wait(handle.job(job))
            .expect("read job")
            .expect("job exists")
            .projects
    };
    assert_eq!(projects_of(parent), [alpha, beta]);
    assert!(
        projects_of(unassociated).is_empty(),
        "a child does not inherit its parent's Projects"
    );
    assert_eq!(projects_of(elsewhere), [gamma]);

    wait(handle.associate_job_project(unassociated, alpha)).expect("associate later");
    assert_eq!(projects_of(unassociated), [alpha]);
    assert_eq!(
        projects_of(parent),
        [alpha, beta],
        "associating a child leaves its parent's set alone"
    );
    assert_eq!(
        violation(wait(handle.associate_job_project(unassociated, alpha))),
        Violation::AlreadyInProject {
            job: unassociated,
            project: alpha
        }
    );
    let missing = ProjectId::new(9_999);
    assert_eq!(
        violation(wait(handle.associate_job_project(unassociated, missing))),
        Violation::NotFound(RecordRef::Project(missing))
    );
    assert_eq!(
        wait(handle.project_jobs(alpha, None, 100)).expect("read Project Jobs"),
        [parent, unassociated]
    );
    assert_eq!(
        wait(handle.project_jobs(gamma, None, 100)).expect("read Project Jobs"),
        [elsewhere]
    );
    worker.shutdown().expect("shutdown");
    assert_relationships_consistent(&directory);
}

#[test]
fn outputs_keep_their_order_and_finalization_stays_atomic() {
    let (_root, directory) = data_directory("outputs");
    let worker = DatabaseWorker::start(&directory).expect("start database");
    let handle = worker.handle();
    let job = local_job(
        &handle,
        new_job(&handle, "orchestrator", handle.genesis(), Vec::new()),
    );
    let child = local_job(&handle, new_job(&handle, "worker", job, Vec::new()));
    for kind in [
        IntermediateOutputKind::Progress,
        IntermediateOutputKind::Evaluation,
    ] {
        wait(handle.append_job_output(
            job,
            NewJobOutput {
                kind,
                summary: None,
                code: None,
                goal_reached: None,
                value: None,
            },
        ))
        .expect("append output");
    }

    // A confirmed outcome over a child that has not settled is recorded as
    // unknown, without a result (I6).
    let early = local_job(
        &handle,
        new_job(&handle, "early", handle.genesis(), Vec::new()),
    );
    local_job(&handle, new_job(&handle, "early child", early, Vec::new()));
    assert_eq!(
        wait(handle.finalize_job(early, JobFinalization::Completed(result("too early"))))
            .expect("finalize"),
        None
    );
    let downgraded = wait(handle.job(early))
        .expect("read job")
        .expect("job exists");
    assert!(
        downgraded.completed_at.is_none() && downgraded.unknown_at.is_some(),
        "the outcome is unknown, not completed"
    );
    assert!(
        downgraded.stop_confirmed_at.is_some() && !downgraded.subtree_settled,
        "its own work has ended but its child's has not"
    );
    assert!(
        wait(handle.job_outputs(early, None, 100))
            .expect("read outputs")
            .is_empty(),
        "and publishes no result"
    );

    let child_result =
        wait(handle.finalize_job(child, JobFinalization::Completed(result("child output"))))
            .expect("finalize child");
    assert_eq!(
        child_result.map(|appended| appended.seq),
        Some(1),
        "the child publishes its own output"
    );
    let appended =
        wait(handle.finalize_job(job, JobFinalization::Completed(result("overall output"))))
            .expect("finalize parent")
            .expect("a confirmed outcome publishes a result");
    let outputs = wait(handle.job_outputs(job, None, 100)).expect("read outputs");
    assert_eq!(
        outputs
            .iter()
            .map(|output| (output.seq, output.kind))
            .collect::<Vec<_>>(),
        [
            (1, OutputKind::Progress),
            (2, OutputKind::Evaluation),
            (3, OutputKind::Result)
        ]
    );
    assert_eq!(
        wait(handle.latest_output(job))
            .expect("read latest")
            .map(|output| output.id),
        Some(appended.output)
    );
    let finalized = wait(handle.job(job))
        .expect("read job")
        .expect("job exists");
    assert_eq!(
        (
            finalized.completed_at,
            finalized.children_closed_at.is_some()
        ),
        (Some(outputs[2].created_at), true)
    );
    assert_eq!(
        violation(wait(handle.append_job_output(
            job,
            NewJobOutput {
                kind: IntermediateOutputKind::Progress,
                summary: None,
                code: None,
                goal_reached: None,
                value: None,
            }
        ))),
        Violation::JobHasTerminalOutcome(job)
    );
    assert_eq!(
        violation(wait(handle.finalize_job(job, JobFinalization::Unknown))),
        Violation::JobAlreadyFinalized(job)
    );
    assert_eq!(
        violation(wait(handle.create_job(new_job(
            &handle,
            "late",
            job,
            Vec::new()
        )))),
        Violation::ParentClosed(job)
    );

    let unknown = local_job(
        &handle,
        new_job(&handle, "unknown", handle.genesis(), Vec::new()),
    );
    assert_eq!(
        wait(handle.finalize_job(unknown, JobFinalization::Unknown))
            .expect("record unknown outcome"),
        None
    );
    assert!(
        wait(handle.job_outputs(unknown, None, 100))
            .expect("read outputs")
            .is_empty()
    );
    worker.shutdown().expect("shutdown");
}

/// A started, configured Job whose provider outcome `error` settles.
fn settled(
    handle: &DatabaseHandle,
    agent: ProfileId,
    targeted: ProfileId,
    executor: ExecutorId,
    error: ProviderExecutionError,
) -> (JobRecord, Vec<AvailabilityStatRecord>) {
    let job = local_job(
        handle,
        NewJob {
            configuration: configuration(agent, targeted),
            ..new_job(handle, "model request", handle.genesis(), Vec::new())
        },
    );
    let input = wait(handle.job(job))
        .expect("read job")
        .expect("job exists")
        .input;
    wait(settle_provider_error(
        handle.clone(),
        executor,
        input,
        job,
        RequestNumber::FIRST,
        error,
    ))
    .expect("settle the provider outcome");
    let record = wait(handle.job(job))
        .expect("read job")
        .expect("job exists");
    let observations =
        wait(handle.job_availability(job, None, 10)).expect("read AvailabilityStats");
    (record, observations)
}

#[test]
fn a_local_stop_settles_unknown_and_records_only_the_response_facts_it_had() {
    let (_root, directory) = data_directory("local-stop");
    let worker = DatabaseWorker::start(&directory).expect("start database");
    let handle = worker.handle();
    let Configured {
        targeted, agent, ..
    } = configured(&handle);
    let executor = executor(&handle, "llm worker", &[OPENAI]).expect("register Executor");
    let facts = |finish_reason, raw_finish_reason: Option<&str>| ResponseFacts {
        status_code: 200,
        finish_reason,
        raw_finish_reason: raw_finish_reason.map(str::to_owned),
    };

    // A user cancellation after the response began: the finish reason was seen.
    let (cancelled, observations) = settled(
        &handle,
        agent,
        targeted,
        executor,
        ProviderExecutionError::Stopped {
            message: "provider execution was cancelled while streaming".into(),
            facts: facts(Some(ChatFinishReason::Stop), Some("stop")),
        },
    );
    assert!(
        cancelled.unknown_at.is_some() && cancelled.failed_at.is_none(),
        "a local stop leaves the Job's outcome unknown, not failed"
    );
    assert_eq!(
        observations
            .iter()
            .map(|record| record.observation.clone())
            .collect::<Vec<_>>(),
        [response(200)],
        "the response facts are kept, and no provider failure is invented"
    );
    assert_eq!(
        observations[0].observation.classification(),
        AvailabilityClassification::Available,
        "a normal finish stays valid after a local stop leaves the Job unknown"
    );

    // A local output-write failure before any finish event.
    let (written, observations) = settled(
        &handle,
        agent,
        targeted,
        executor,
        ProviderExecutionError::Stopped {
            message: "database could not persist provider output".into(),
            facts: facts(None, None),
        },
    );
    assert!(written.unknown_at.is_some() && written.failed_at.is_none());
    assert_eq!(
        observations
            .iter()
            .map(|record| record.observation.clone())
            .collect::<Vec<_>>(),
        [AvailabilityObservation {
            status_code: Some(200),
            finish_reason: None,
            raw_finish_reason: None,
            failure: None,
        }],
        "a local write failure is not a provider failure"
    );
    assert_eq!(
        observations[0].observation.classification(),
        AvailabilityClassification::Unconfirmed,
        "a local write failure without a finish reason is not availability evidence"
    );
    worker.shutdown().expect("shutdown");
}

#[test]
fn provider_and_transport_failures_are_observed_and_refused_requests_are_not() {
    let (_root, directory) = data_directory("provider-failures");
    let worker = DatabaseWorker::start(&directory).expect("start database");
    let handle = worker.handle();
    let Configured {
        targeted, agent, ..
    } = configured(&handle);
    let executor = executor(&handle, "llm worker", &[OPENAI]).expect("register Executor");

    let transport = ProviderExecutionError::Unknown {
        message: "connection reset".into(),
        facts: None,
        finished: false,
    };
    let transport_message = transport.to_string();
    let (job, observations) = settled(&handle, agent, targeted, executor, transport);
    assert!(job.unknown_at.is_some() && job.failed_at.is_none());
    assert_eq!(
        observations
            .iter()
            .map(|record| record.observation.clone())
            .collect::<Vec<_>>(),
        [failure(ProviderFailure {
            message: transport_message,
            ..ProviderFailure::default()
        })],
        "a transport failure is a genuine observation"
    );
    assert_eq!(
        observations[0].observation.classification(),
        AvailabilityClassification::Unconfirmed,
        "a transport failure without an observed status is not proof of unavailability"
    );

    let rejected = provider_failure("rate limited", Some(429), true);
    let (job, observations) = settled(
        &handle,
        agent,
        targeted,
        executor,
        ProviderExecutionError::Rejected {
            failure: rejected.clone(),
        },
    );
    assert!(job.failed_at.is_some() && job.unknown_at.is_none());
    assert_eq!(
        observations
            .iter()
            .map(|record| record.observation.clone())
            .collect::<Vec<_>>(),
        [failure(rejected)],
        "a genuine HTTP rejection keeps its status and failure"
    );
    assert_eq!(
        observations[0].observation.classification(),
        AvailabilityClassification::Rejected(RejectionCause::RateLimited),
        "a rate-limit rejection is a definite refusal, not unavailability"
    );

    let server_error = provider_failure("overloaded", Some(503), true);
    let (job, observations) = settled(
        &handle,
        agent,
        targeted,
        executor,
        ProviderExecutionError::Rejected {
            failure: server_error.clone(),
        },
    );
    assert!(job.failed_at.is_some() && job.unknown_at.is_none());
    assert_eq!(
        observations[0].observation.classification(),
        AvailabilityClassification::Unavailable(UnavailableCause::ProviderServerError),
        "an observed HTTP 5xx is Provider unavailability"
    );

    for error in [
        ProviderExecutionError::NotDelivered {
            message: "refused".into(),
        },
        ProviderExecutionError::Build {
            message: "bad request".into(),
        },
    ] {
        let (job, observations) = settled(&handle, agent, targeted, executor, error);
        assert!(job.failed_at.is_some() && job.unknown_at.is_none());
        assert!(
            observations.is_empty(),
            "a request refused before delivery has no observation"
        );
    }
    worker.shutdown().expect("shutdown");
}

/// Records one observation of `job`, observed at `micros`.
fn observe(handle: &DatabaseHandle, job: JobId, micros: i64, observation: AvailabilityObservation) {
    wait(handle.record_availability_stat(NewAvailabilityStat {
        job,
        request: RequestNumber::FIRST,
        observed_at: at(micros),
        observation,
    }))
    .expect("record AvailabilityStat");
}

/// The Job's raw AvailabilityStats, in recording order.
fn stats(handle: &DatabaseHandle, job: JobId) -> Vec<AvailabilityStatRecord> {
    wait(handle.job_availability(job, None, 100)).expect("read AvailabilityStats")
}

/// The sample of records that all belong to one Job.
fn sample_of<'a>(
    records: impl IntoIterator<Item = &'a AvailabilityStatRecord>,
) -> AvailabilityRequestSample {
    AvailabilityRequestSample::from_records(records)
        .expect("one Job")
        .expect("records exist")
}

/// An HTTP status with no finish reason and no failure.
fn bare(status_code: u16) -> AvailabilityObservation {
    AvailabilityObservation {
        status_code: Some(status_code),
        finish_reason: None,
        raw_finish_reason: None,
        failure: None,
    }
}

#[test]
fn a_request_is_available_once_a_normal_finish_is_observed() {
    let (_root, directory) = data_directory("request-sample-finish");
    let worker = DatabaseWorker::start(&directory).expect("start database");
    let handle = worker.handle();
    let Configured {
        targeted, agent, ..
    } = configured(&handle);

    let stop = configured_child(&handle, agent, targeted);
    observe(&handle, stop, 100, bare(200));
    assert_eq!(
        sample_of(&stats(&handle, stop)).classification,
        AvailabilityClassification::Unconfirmed,
        "HTTP 200 alone is not completion"
    );
    observe(&handle, stop, 200, response(200));
    let records = stats(&handle, stop);
    let sample = sample_of(&records);
    assert_eq!(
        (
            sample.job,
            sample.first_record,
            sample.observed_at,
            sample.classification
        ),
        (
            stop,
            records[0].id,
            at(100),
            AvailabilityClassification::Available
        ),
        "a Stop finish after a bare 200 makes the request available"
    );

    let length = configured_child(&handle, agent, targeted);
    observe(&handle, length, 100, bare(200));
    observe(
        &handle,
        length,
        200,
        AvailabilityObservation {
            status_code: Some(200),
            finish_reason: Some(ChatFinishReason::Length),
            raw_finish_reason: Some("length".into()),
            failure: None,
        },
    );
    assert_eq!(
        sample_of(&stats(&handle, length)).classification,
        AvailabilityClassification::Available,
        "a Length finish is a normal termination too"
    );
    worker.shutdown().expect("shutdown");
}

#[test]
fn confirmed_completion_is_not_downgraded_by_missing_or_transport_only_evidence() {
    let (_root, directory) = data_directory("request-sample-completion");
    let worker = DatabaseWorker::start(&directory).expect("start database");
    let handle = worker.handle();
    let Configured {
        targeted, agent, ..
    } = configured(&handle);
    let job = configured_child(&handle, agent, targeted);

    observe(&handle, job, 100, response(200));
    observe(
        &handle,
        job,
        200,
        failure(ProviderFailure {
            message: "connection reset".into(),
            ..ProviderFailure::default()
        }),
    );
    observe(
        &handle,
        job,
        300,
        failure(ProviderFailure {
            message: "stream ended early".into(),
            status_code: Some(200),
            ..ProviderFailure::default()
        }),
    );
    observe(&handle, job, 400, response(200));

    let records = stats(&handle, job);
    for prefix in 1..=records.len() {
        assert_eq!(
            sample_of(&records[..prefix]).classification,
            AvailabilityClassification::Available,
            "a valid normal termination survives each later observation, {prefix} seen"
        );
    }
    let replayed = records.iter().chain(&records);
    assert_eq!(
        sample_of(replayed),
        sample_of(&records),
        "repeated evidence reaches the same sample"
    );
    worker.shutdown().expect("shutdown");
}

#[test]
fn a_provider_fault_or_rejection_is_not_erased_by_missing_evidence() {
    let (_root, directory) = data_directory("request-sample-fault");
    let worker = DatabaseWorker::start(&directory).expect("start database");
    let handle = worker.handle();
    let Configured {
        targeted, agent, ..
    } = configured(&handle);
    let transport = || {
        failure(ProviderFailure {
            message: "connection reset".into(),
            ..ProviderFailure::default()
        })
    };

    let unavailable = configured_child(&handle, agent, targeted);
    observe(
        &handle,
        unavailable,
        100,
        failure(provider_failure("overloaded", Some(503), true)),
    );
    observe(&handle, unavailable, 200, transport());
    assert_eq!(
        sample_of(&stats(&handle, unavailable)).classification,
        AvailabilityClassification::Unavailable(UnavailableCause::ProviderServerError),
        "transport-only evidence after an observed 5xx does not erase it"
    );

    let rejected = configured_child(&handle, agent, targeted);
    observe(
        &handle,
        rejected,
        100,
        failure(provider_failure("rate limited", Some(429), true)),
    );
    observe(&handle, rejected, 200, transport());
    assert_eq!(
        sample_of(&stats(&handle, rejected)).classification,
        AvailabilityClassification::Rejected(RejectionCause::RateLimited),
        "transport-only evidence after a rejection does not erase it"
    );
    worker.shutdown().expect("shutdown");
}

#[test]
fn explicit_contradictions_make_a_request_unconfirmed_and_keep_its_history() {
    let (_root, directory) = data_directory("request-sample-conflict");
    let worker = DatabaseWorker::start(&directory).expect("start database");
    let handle = worker.handle();
    let Configured {
        targeted, agent, ..
    } = configured(&handle);
    let cases = [
        (
            "two HTTP statuses",
            vec![
                response(200),
                failure(provider_failure("overloaded", Some(503), true)),
            ],
        ),
        (
            "two finish reasons",
            vec![
                response(200),
                AvailabilityObservation {
                    status_code: Some(200),
                    finish_reason: Some(ChatFinishReason::Length),
                    raw_finish_reason: Some("length".into()),
                    failure: None,
                },
            ],
        ),
        (
            "a raw finish reason naming another reason",
            vec![
                response(200),
                AvailabilityObservation {
                    status_code: Some(200),
                    finish_reason: Some(ChatFinishReason::Stop),
                    raw_finish_reason: Some("length".into()),
                    failure: None,
                },
            ],
        ),
        (
            "a failure status that differs from a supplied 200",
            vec![
                response(200),
                AvailabilityObservation {
                    status_code: None,
                    finish_reason: None,
                    raw_finish_reason: None,
                    failure: Some(provider_failure("overloaded", Some(503), true)),
                },
            ],
        ),
    ];
    for (case, observations) in cases {
        let job = configured_child(&handle, agent, targeted);
        for (index, observation) in observations.iter().enumerate() {
            observe(&handle, job, (index as i64 + 1) * 100, observation.clone());
        }
        let records = stats(&handle, job);
        assert_eq!(
            records
                .iter()
                .map(|record| record.observation.clone())
                .collect::<Vec<_>>(),
            observations,
            "the raw history of {case} is kept as recorded"
        );
        assert_eq!(
            sample_of(&records).classification,
            AvailabilityClassification::Unconfirmed,
            "{case} is an explicit contradiction"
        );
    }
    worker.shutdown().expect("shutdown");
}

#[test]
fn the_first_record_is_the_lowest_id_not_the_earliest_observation() {
    let (_root, directory) = data_directory("request-sample-first");
    let worker = DatabaseWorker::start(&directory).expect("start database");
    let handle = worker.handle();
    let Configured {
        targeted, agent, ..
    } = configured(&handle);
    let job = configured_child(&handle, agent, targeted);
    observe(&handle, job, 200, bare(200));
    observe(&handle, job, 100, response(200));

    let records = stats(&handle, job);
    let forward = sample_of(&records);
    assert_eq!(
        (
            forward.first_record,
            forward.observed_at,
            forward.classification
        ),
        (
            records[0].id,
            at(200),
            AvailabilityClassification::Available
        ),
        "the later record with the earlier observed_at does not move the sample"
    );
    assert_eq!(
        sample_of(records.iter().rev()),
        forward,
        "replay in any order reaches the same sample"
    );
    worker.shutdown().expect("shutdown");
}

/// The sample of records given in recording order, which must be the same
/// sample when they are replayed in reverse.
fn sample_in_either_order(records: &[AvailabilityStatRecord]) -> AvailabilityRequestSample {
    let forward = sample_of(records);
    assert_eq!(
        sample_of(records.iter().rev()),
        forward,
        "replay in reverse order reaches the same sample"
    );
    forward
}

#[test]
fn compatible_facts_split_across_records_make_a_request_available_in_either_order() {
    let (_root, directory) = data_directory("request-sample-split");
    let worker = DatabaseWorker::start(&directory).expect("start database");
    let handle = worker.handle();
    let Configured {
        targeted, agent, ..
    } = configured(&handle);
    let transport = failure(ProviderFailure {
        message: "connection reset".into(),
        ..ProviderFailure::default()
    });
    for (finish, raw) in [
        (ChatFinishReason::Stop, "stop"),
        (ChatFinishReason::Length, "length"),
    ] {
        let job = configured_child(&handle, agent, targeted);
        observe(&handle, job, 100, bare(200));
        observe(
            &handle,
            job,
            200,
            AvailabilityObservation {
                status_code: None,
                finish_reason: Some(finish),
                raw_finish_reason: Some(raw.into()),
                failure: None,
            },
        );
        observe(&handle, job, 300, transport.clone());

        let records = stats(&handle, job);
        for record in &records {
            assert_eq!(
                sample_of(std::slice::from_ref(record)).classification,
                AvailabilityClassification::Unconfirmed,
                "each record is Unconfirmed on its own"
            );
        }
        let sample = sample_in_either_order(&records);
        assert_eq!(
            (
                sample.first_record,
                sample.observed_at,
                sample.classification
            ),
            (
                records[0].id,
                at(100),
                AvailabilityClassification::Available
            ),
            "a 200 and a normal {raw} finish in separate records are one completion"
        );
        assert_eq!(
            sample_of(records.iter().chain(&records)),
            sample,
            "duplicate evidence adds no request"
        );
    }
    worker.shutdown().expect("shutdown");
}

#[test]
fn an_invalid_terminal_observation_is_no_witness_even_when_combined() {
    let (_root, directory) = data_directory("request-sample-invalid-terminal");
    let worker = DatabaseWorker::start(&directory).expect("start database");
    let handle = worker.handle();
    let Configured {
        targeted, agent, ..
    } = configured(&handle);
    let stream_reset = || {
        Some(ProviderFailure {
            message: "stream reset".into(),
            ..ProviderFailure::default()
        })
    };
    let invalid = [
        (
            "a normal finish beside a failure",
            AvailabilityObservation {
                status_code: Some(200),
                finish_reason: Some(ChatFinishReason::Stop),
                raw_finish_reason: Some("stop".into()),
                failure: stream_reset(),
            },
        ),
        (
            "a normal finish without status beside a failure",
            AvailabilityObservation {
                status_code: None,
                finish_reason: Some(ChatFinishReason::Stop),
                raw_finish_reason: Some("stop".into()),
                failure: stream_reset(),
            },
        ),
    ];
    for (case, terminal) in invalid {
        let job = configured_child(&handle, agent, targeted);
        observe(&handle, job, 100, terminal);
        observe(&handle, job, 200, bare(200));
        let records = stats(&handle, job);
        assert_eq!(
            sample_of(&records[..1]).classification,
            AvailabilityClassification::Unconfirmed,
            "{case} is Unconfirmed alone"
        );
        assert_eq!(
            sample_in_either_order(&records).classification,
            AvailabilityClassification::Unconfirmed,
            "{case} does not become a witness when a 200 is combined with it"
        );
    }
    worker.shutdown().expect("shutdown");
}

#[test]
fn a_failure_status_conflicts_only_with_a_different_supplied_status() {
    let (_root, directory) = data_directory("request-sample-failure-status");
    let worker = DatabaseWorker::start(&directory).expect("start database");
    let handle = worker.handle();
    let Configured {
        targeted, agent, ..
    } = configured(&handle);
    let failure_only = || AvailabilityObservation {
        status_code: None,
        finish_reason: None,
        raw_finish_reason: None,
        failure: Some(provider_failure("overloaded", Some(503), true)),
    };

    let alone = configured_child(&handle, agent, targeted);
    observe(&handle, alone, 100, failure_only());
    assert_eq!(
        sample_in_either_order(&stats(&handle, alone)).classification,
        AvailabilityClassification::Unconfirmed,
        "a failure status without an observed status is not classified"
    );

    let compatible = configured_child(&handle, agent, targeted);
    observe(&handle, compatible, 100, failure_only());
    observe(&handle, compatible, 200, bare(503));
    assert_eq!(
        sample_in_either_order(&stats(&handle, compatible)).classification,
        AvailabilityClassification::Unavailable(UnavailableCause::ProviderServerError),
        "a later observed 503 is compatible with an earlier failure that omitted its status"
    );

    let different = configured_child(&handle, agent, targeted);
    observe(&handle, different, 100, failure_only());
    observe(&handle, different, 200, bare(200));
    assert_eq!(
        sample_in_either_order(&stats(&handle, different)).classification,
        AvailabilityClassification::Unconfirmed,
        "a supplied 200 differs from the failure's 503"
    );

    let finish_only = configured_child(&handle, agent, targeted);
    observe(&handle, finish_only, 100, failure_only());
    observe(
        &handle,
        finish_only,
        200,
        AvailabilityObservation {
            status_code: None,
            finish_reason: Some(ChatFinishReason::Stop),
            raw_finish_reason: Some("stop".into()),
            failure: None,
        },
    );
    assert_eq!(
        sample_in_either_order(&stats(&handle, finish_only)).classification,
        AvailabilityClassification::Unconfirmed,
        "a finish without an observed response status is not a completion"
    );
    worker.shutdown().expect("shutdown");
}

// ---- Token annotations and capture ------------------------------------------------

use crate::openai_compatible::ChatConversation;
use crate::orchestration::execution_runtime::{finish_provider_job, output_sink};
use crate::persistence::Measurements as TokenMeasurements;
use crate::provider_execution::scripted::{Step, payload};
use crate::provider_execution::{
    CancellationToken, CredentialResolver, ProviderExecutionConfig, ProviderFact, announce,
    execute_chat_with_sink, read_response,
};
use crate::token_usage::{
    RequestEligibility, TokenAnnotation, TokenCount, TokenRequestSample, TokenUsageState,
    UsageFinality,
};
use std::time::{Duration, Instant};

/// Metrics with the given counters, annotated or not.
fn token_metrics(
    job: JobId,
    micros: i64,
    token: Option<TokenAnnotation>,
    counters: [Option<u32>; 2],
) -> NewMetrics {
    NewMetrics {
        job,
        request: RequestNumber::FIRST,
        observed_at: at(micros),
        measurements: TokenMeasurements {
            usage: NormalizedUsage {
                input_tokens: counters[0],
                output_tokens: counters[1],
                ..NormalizedUsage::default()
            },
            ..TokenMeasurements::default()
        },
        token,
    }
}

fn job_metrics(handle: &DatabaseHandle, job: JobId) -> Vec<crate::persistence::MetricsRecord> {
    let mut all: Vec<crate::persistence::MetricsRecord> = Vec::new();
    loop {
        let page = wait(handle.job_metrics(job, all.last().map(|record| record.id), 1_000))
            .expect("read Metrics");
        let done = page.len() < 1_000;
        all.extend(page);
        if done {
            return all;
        }
    }
}

fn stored_sample(handle: &DatabaseHandle, job: JobId) -> Option<TokenRequestSample> {
    TokenRequestSample::from_metrics(&job_metrics(handle, job)).expect("one Job")
}

#[test]
fn token_annotations_are_validated_stored_and_survive_reopening() {
    let (_root, directory) = data_directory("token-annotations");
    let worker = DatabaseWorker::start(&directory).expect("start database");
    let handle = worker.handle();
    let (provider, llm) = llm_setup(&handle, "model-a");
    let llm_job = llm_job(&handle, llm);
    let plain = create_job(
        &handle,
        new_job(&handle, "plain", handle.genesis(), Vec::new()),
    );
    let eligible = TokenAnnotation::eligible(UsageFinality::Cumulative);

    // Generic Metrics need no annotation, on an LLM Job or not, and an empty
    // unannotated record is still refused.
    for job in [llm_job, plain] {
        wait(handle.record_metrics(token_metrics(job, 10, None, [Some(1), Some(2)])))
            .expect("unannotated Metrics are generic Metrics");
    }
    assert!(matches!(
        violation(wait(handle.record_metrics(token_metrics(
            llm_job,
            11,
            None,
            [None, None]
        )))),
        Violation::InvalidValue("measurements", _)
    ));

    // An eligible or excluded fact needs no measurement. One Job's facts
    // agree on whether its request was sent, so the excluded fact belongs to
    // a request that never was.
    let anchor =
        wait(handle.record_metrics(token_metrics(llm_job, 20, Some(eligible), [None, None])))
            .expect("an eligible fact-only record");
    let unsent = self::llm_job(&handle, llm);
    let excluded = wait(handle.record_metrics(token_metrics(
        unsent,
        21,
        Some(TokenAnnotation::excluded()),
        [None, None],
    )))
    .expect("an excluded fact-only record");
    wait(handle.record_metrics(token_metrics(
        llm_job,
        22,
        Some(TokenAnnotation::eligible(UsageFinality::Final)),
        [Some(7), None],
    )))
    .expect("a final count with the counters it reports");

    // Refusals: an annotation needs the Job's frozen LLM, and an unsent
    // request reports no usage and no final.
    let before = job_metrics(&handle, llm_job).len();
    for refused in [
        token_metrics(
            llm_job,
            31,
            Some(TokenAnnotation::excluded()),
            [Some(1), None],
        ),
        token_metrics(
            llm_job,
            32,
            Some(TokenAnnotation {
                eligibility: RequestEligibility::Excluded,
                finality: UsageFinality::Final,
            }),
            [None, None],
        ),
    ] {
        assert!(matches!(
            violation(wait(handle.record_metrics(refused))),
            Violation::InvalidValue("token", _)
        ));
    }
    assert_eq!(
        job_metrics(&handle, llm_job).len(),
        before,
        "refusals wrote nothing"
    );
    assert_eq!(
        job_metrics(&handle, plain).len(),
        1,
        "no partial link or index entry"
    );

    // The identity of an annotated record is the Job's frozen target.
    let records = job_metrics(&handle, llm_job);
    let target = records[1].target.clone().expect("the Job's frozen target");
    assert_eq!(
        (
            target.llm_profile,
            target.frozen.provider.id,
            target.frozen.config.model.as_str()
        ),
        (llm, provider, "model-a")
    );
    assert_eq!(
        records
            .iter()
            .map(|record| record.token)
            .collect::<Vec<_>>(),
        [
            None,
            Some(eligible),
            Some(TokenAnnotation::eligible(UsageFinality::Final)),
        ]
    );
    assert_eq!(records[1].id, anchor);
    assert_eq!(
        job_metrics(&handle, unsent)
            .iter()
            .map(|record| (record.id, record.token))
            .collect::<Vec<_>>(),
        [(excluded, Some(TokenAnnotation::excluded()))]
    );
    let page = wait(handle.metrics(of_profile(llm), None, 100)).expect("scope read");
    assert_eq!(
        page.records
            .iter()
            .filter(|record| record.job == llm_job)
            .map(|record| record.token)
            .collect::<Vec<_>>(),
        records
            .iter()
            .map(|record| record.token)
            .collect::<Vec<_>>(),
        "the scope index serves the annotations too"
    );

    worker.shutdown().expect("shutdown");
    assert_relationships_consistent(&directory);
    let worker = DatabaseWorker::start(&directory).expect("reopen database");
    let handle = worker.handle();
    assert_eq!(
        job_metrics(&handle, llm_job),
        records,
        "the round trip is exact"
    );
    worker.shutdown().expect("shutdown");
}

/// A started Job that runs `llm` directly, and the Executor settling it.
#[derive(Clone, Copy)]
struct TokenRun {
    executor: ExecutorId,
    job: JobId,
    input: crate::persistence::JobInputId,
}

fn token_run(handle: &DatabaseHandle, llm: ProfileId) -> TokenRun {
    let executor = executor(handle, "llm worker", &[OPENAI]).expect("register Executor");
    let job = llm_job(handle, llm);
    wait(handle.claim_fixture(job, test_executor(handle))).expect("start job");
    let input = wait(handle.job(job))
        .expect("read job")
        .expect("job exists")
        .input;
    TokenRun {
        executor,
        job,
        input,
    }
}

fn on_ntex<F: std::future::Future + 'static>(future: F) -> F::Output
where
    F::Output: 'static,
{
    ntex::rt::System::new("token-test", ntex::rt::DefaultRuntime).block_on(future)
}

/// The execution path after a response head: announce the delivered request,
/// read the scripted body with the real parser into the real sink, and settle.
fn drive(
    handle: &DatabaseHandle,
    run: TokenRun,
    steps: Vec<Step>,
    timeout: Duration,
) -> Result<crate::orchestration::execution_runtime::ExecutionReceipt, ExecutionRuntimeError> {
    let config = ProviderExecutionConfig {
        timeout,
        cancellation: CancellationToken::default(),
        ..ProviderExecutionConfig::default()
    };
    // A Job objective offers no tools.
    drive_with(handle, run, steps, config, ChatConversation::default())
}

/// Like [`drive`], with the execution's configuration and the conversation
/// whose tools the response may call given.
fn drive_with(
    handle: &DatabaseHandle,
    run: TokenRun,
    steps: Vec<Step>,
    config: ProviderExecutionConfig,
    conversation: ChatConversation,
) -> Result<crate::orchestration::execution_runtime::ExecutionReceipt, ExecutionRuntimeError> {
    let handle = handle.clone();
    on_ntex(async move {
        let mut sink = output_sink(handle.clone(), run.job);
        announce(Some(&mut sink), ProviderFact::Delivered).await;
        let execution = read_response(
            payload(steps, config.cancellation.clone()),
            200,
            Instant::now(),
            None,
            &config,
            Some(&mut sink),
            &conversation,
        )
        .await;
        finish_provider_job(handle, run.executor, run.input, run.job, sink, execution).await
    })
}

const BOUND: Duration = Duration::from_secs(5);
const CUM_10: &str = "data: {\"choices\":[{\"delta\":{\"content\":\"a\"}}],\"usage\":{\"prompt_tokens\":10,\"completion_tokens\":1}}\n\n";
const CUM_25: &str = "data: {\"choices\":[{\"delta\":{\"content\":\"b\"}}],\"usage\":{\"prompt_tokens\":25,\"completion_tokens\":2}}\n\n";
const STOP_CHUNK: &str = "data: {\"choices\":[{\"delta\":{},\"finish_reason\":\"stop\"}]}\n\n";
const FINAL_40_60: &str = "data: {\"choices\":[],\"usage\":{\"prompt_tokens\":4000000000,\"completion_tokens\":2000000000}}\n\n";
const FINAL_40_61: &str = "data: {\"choices\":[],\"usage\":{\"prompt_tokens\":4000000000,\"completion_tokens\":2000000001}}\n\n";
const FINAL_INPUT_ONLY: &str = "data: {\"choices\":[],\"usage\":{\"prompt_tokens\":40}}\n\n";
const FINAL_ZERO_OUTPUT: &str =
    "data: {\"choices\":[],\"usage\":{\"prompt_tokens\":0,\"completion_tokens\":0}}\n\n";
const DONE_CHUNK: &str = "data: [DONE]\n\n";

#[test]
fn streamed_text_is_coalesced_into_fewer_durable_outputs_without_changing_reconstruction() {
    const DELTAS: usize = 128;
    let (_root, directory) = data_directory("provider-output-coalescing");
    let worker = DatabaseWorker::start(&directory).expect("start database");
    let handle = worker.handle();
    let (_provider, llm) = llm_setup(&handle, "model-a");
    let run = token_run(&handle, llm);
    let mut steps = (0..DELTAS)
        .map(|_| {
            Step::Chunk(leaked_chunk(
                "data: {\"choices\":[{\"delta\":{\"content\":\"x\"}}]}\n\n".to_owned(),
            ))
        })
        .collect::<Vec<_>>();
    steps.push(Step::Chunk(STOP_CHUNK));
    let started = Instant::now();
    drive(&handle, run, steps, BOUND).expect("settle the run");
    let elapsed = started.elapsed();

    let outputs = wait(handle.job_outputs(run.job, None, 10)).expect("read outputs");
    let progress = outputs
        .iter()
        .filter(|output| output.kind == OutputKind::Progress)
        .collect::<Vec<_>>();
    assert_eq!(
        progress.len(),
        1,
        "128 deltas require one durable Progress write"
    );
    assert_eq!(
        progress
            .iter()
            .filter_map(|output| output.summary.as_deref())
            .collect::<String>(),
        "x".repeat(DELTAS),
        "durable Progress records reconstruct the provider text exactly"
    );
    assert_eq!(
        outputs.iter().map(|output| output.kind).collect::<Vec<_>>(),
        [OutputKind::Progress, OutputKind::Result],
        "the final Result follows all durable Progress output"
    );
    assert_eq!(
        outputs[1].summary.as_deref(),
        Some("x".repeat(DELTAS).as_str()),
        "the public final summary contract remains unchanged"
    );
    assert_eq!(
        outputs[1]
            .value
            .as_ref()
            .and_then(|value| value.get("text"))
            .and_then(serde_json::Value::as_str),
        Some("x".repeat(DELTAS).as_str()),
        "the structured final-result contract remains unchanged"
    );
    println!(
        "provider stream measurement: deltas={DELTAS} progress_writes={} elapsed={elapsed:?}",
        progress.len()
    );
    worker.shutdown().expect("shutdown");
}

/// Runs a scripted body, then reopens the database and reduces what was
/// stored, with no eligibility or finality supplied by the test.
fn run_and_reduce(
    name: &str,
    steps: Vec<Step>,
    timeout: Duration,
) -> (
    Option<TokenRequestSample>,
    JobRecord,
    Vec<crate::persistence::MetricsRecord>,
) {
    let (sample, job, records, _) = run_and_inspect(name, steps, timeout);
    (sample, job, records)
}

/// Like [`run_and_reduce`], and also reads the Job's availability records.
fn run_and_inspect(
    name: &str,
    steps: Vec<Step>,
    timeout: Duration,
) -> (
    Option<TokenRequestSample>,
    JobRecord,
    Vec<crate::persistence::MetricsRecord>,
    Vec<AvailabilityStatRecord>,
) {
    let (_root, directory) = data_directory(name);
    let worker = DatabaseWorker::start(&directory).expect("start database");
    let handle = worker.handle();
    let (_provider, llm) = llm_setup(&handle, "model-a");
    let run = token_run(&handle, llm);
    drive(&handle, run, steps, timeout).expect("settle the run");
    worker.shutdown().expect("shutdown");

    let worker = DatabaseWorker::start(&directory).expect("reopen database");
    let handle = worker.handle();
    let records = job_metrics(&handle, run.job);
    let job = wait(handle.job(run.job))
        .expect("read job")
        .expect("job exists");
    let sample = TokenRequestSample::from_metrics(&records).expect("one Job");
    let availability =
        wait(handle.job_availability(run.job, None, 100)).expect("read AvailabilityStats");
    worker.shutdown().expect("shutdown");
    (sample, job, records, availability)
}

#[test]
fn streamed_usage_reduces_after_reopening_to_one_complete_request() {
    let (sample, job, records) = run_and_reduce(
        "token-complete",
        vec![
            Step::Chunk(CUM_10),
            Step::Chunk(CUM_25),
            Step::Chunk(STOP_CHUNK),
            Step::Chunk(FINAL_40_60),
            Step::Chunk(DONE_CHUNK),
        ],
        BOUND,
    );
    let sample = sample.expect("one request sample");
    assert!(job.completed_at.is_some());
    assert_eq!(
        (sample.input, sample.output, sample.usage),
        (
            TokenCount::Final(4_000_000_000),
            TokenCount::Final(2_000_000_000),
            TokenUsageState::Complete
        ),
        "the whole-request chunk replaces the running totals, which are never added"
    );
    assert_eq!(
        sample.total,
        Some(6_000_000_000),
        "the total is exact above u32::MAX"
    );
    let annotated: Vec<_> = records
        .iter()
        .filter(|record| record.token.is_some())
        .collect();
    assert_eq!(
        annotated.len(),
        4,
        "the anchor, two running totals and the final"
    );
    assert_eq!(
        (sample.first_metrics, sample.observed_at),
        (annotated[0].id, annotated[0].observed_at),
        "the sample is fixed by the lowest Metrics ID, the eligible anchor"
    );
    let generic: Vec<_> = records
        .iter()
        .filter(|record| record.token.is_none())
        .collect();
    assert_eq!(generic.len(), 1, "one generic timing record");
    let timing = &generic[0].measurements;
    assert!(
        timing.ttfb_ms.is_some() && timing.request_finished_in_ms.is_some(),
        "it keeps the request's timing"
    );
    assert_eq!(
        timing.usage,
        NormalizedUsage::default(),
        "and no second copy of the usage counters or the raw object"
    );
    let annotated_only: Vec<_> = records
        .iter()
        .filter(|record| record.token.is_some())
        .cloned()
        .collect();
    assert_eq!(
        TokenRequestSample::from_metrics(&annotated_only).expect("one Job"),
        Some(sample),
        "the annotated records alone give the same request-level result"
    );
}

/// Runs a scripted body as a Job objective and keeps everything durable it left.
fn run_objective(
    name: &str,
    steps: Vec<Step>,
) -> (
    JobRecord,
    Vec<crate::persistence::JobOutputRecord>,
    Option<TokenRequestSample>,
    Option<AvailabilityRequestSample>,
) {
    let (_root, directory) = data_directory(name);
    let worker = DatabaseWorker::start(&directory).expect("start database");
    let handle = worker.handle();
    let (_provider, llm) = llm_setup(&handle, "model-a");
    let run = token_run(&handle, llm);
    drive(&handle, run, steps, BOUND).expect("settle the run");
    let job = wait(handle.job(run.job))
        .expect("read job")
        .expect("job exists");
    let outputs = wait(handle.job_outputs(run.job, None, 100)).expect("read outputs");
    let tokens = TokenRequestSample::from_metrics(&job_metrics(&handle, run.job)).expect("one Job");
    let availability =
        wait(handle.job_availability(run.job, None, 100)).expect("read AvailabilityStats");
    let availability = AvailabilityRequestSample::from_records(&availability).expect("one Job");
    worker.shutdown().expect("shutdown");
    (job, outputs, tokens, availability)
}

#[test]
fn a_tool_call_answer_to_a_job_objective_never_completes_the_job_or_fabricates_output() {
    const TOOL_CALL: &str = "data: {\"choices\":[{\"delta\":{\"tool_calls\":[{\"index\":0,\"id\":\"call_1\",\"type\":\"function\",\"function\":{\"name\":\"f\",\"arguments\":\"{}\"}}]}}]}\n\n";
    const TOOL_CALLS_FINISH: &str =
        "data: {\"choices\":[{\"delta\":{},\"finish_reason\":\"tool_calls\"}]}\n\n";
    let (job, outputs, tokens, availability) = run_objective(
        "tool-call-objective",
        vec![
            Step::Chunk(CUM_10),
            Step::Chunk(TOOL_CALL),
            Step::Chunk(TOOL_CALLS_FINISH),
            Step::Chunk(FINAL_40_60),
            Step::Chunk(DONE_CHUNK),
        ],
    );
    assert!(
        job.completed_at.is_none() && job.failed_at.is_none(),
        "a Job objective offers no tools, so a tool call is no answer to it"
    );
    assert!(
        job.unknown_at.is_some(),
        "the exchange is an incomplete stream"
    );
    assert!(
        job.stop_confirmed_at.is_some(),
        "and its local stop is confirmed"
    );
    assert_eq!(
        outputs
            .iter()
            .map(|output| (output.kind, output.summary.as_deref()))
            .collect::<Vec<_>>(),
        [(OutputKind::Progress, Some("a"))],
        "the streamed text is kept, and no result or tool output is fabricated"
    );
    assert_eq!(
        availability.map(|sample| sample.classification),
        Some(AvailabilityClassification::Unconfirmed)
    );

    let (text_job, _, text_tokens, _) = run_objective(
        "tool-call-objective-text",
        vec![
            Step::Chunk(CUM_10),
            Step::Chunk(STOP_CHUNK),
            Step::Chunk(FINAL_40_60),
            Step::Chunk(DONE_CHUNK),
        ],
    );
    assert!(
        text_job.completed_at.is_some(),
        "the text answer still completes"
    );
    let tokens = tokens.expect("a Token sample");
    let text_tokens = text_tokens.expect("a Token sample");
    assert_eq!(
        (tokens.input, tokens.output, tokens.usage, tokens.total),
        (
            text_tokens.input,
            text_tokens.output,
            text_tokens.usage,
            text_tokens.total
        ),
        "the Token evidence of the request is the same either way"
    );
}

/// A distinct, non-monotonic usage object for the nth chunk, as JSON text.
fn varied_usage(n: u32) -> String {
    let input = (n * 7) % 13 + 1;
    let output = (n * 5) % 11;
    match n % 3 {
        0 => format!("{{\"prompt_tokens\":{input},\"completion_tokens\":{output}}}"),
        1 => format!("{{\"completion_tokens\":{output},\"extra\":{n}}}"),
        _ => format!(
            "{{\"prompt_tokens\":{input},\"prompt_tokens_details\":{{\"cached_tokens\":{}}}}}",
            n % 4
        ),
    }
}

fn leaked_chunk(chunk: String) -> &'static str {
    Box::leak(chunk.into_boxed_str())
}

#[test]
fn every_source_usage_object_survives_reopening_unmerged_with_its_own_fields() {
    const OBJECTS: u32 = 34;
    let sources: Vec<String> = (0..OBJECTS).map(varied_usage).collect();
    let mut steps: Vec<Step> = sources
        .iter()
        .map(|usage| {
            Step::Chunk(leaked_chunk(format!(
                "data: {{\"choices\":[{{\"delta\":{{}}}}],\"usage\":{usage}}}\n\n"
            )))
        })
        .collect();
    steps.push(Step::Chunk(STOP_CHUNK));
    let (sample, job, records, availability) =
        run_and_inspect("token-source-objects", steps, BOUND);
    assert!(job.completed_at.is_some());
    assert_eq!(availability.len(), 1);

    let usages: Vec<_> = records
        .iter()
        .filter(|record| record.token.is_some() && record.measurements.usage.raw.is_some())
        .collect();
    assert_eq!(
        usages.len(),
        OBJECTS as usize,
        "no synthetic or missing row"
    );
    for (record, source) in usages.iter().zip(&sources) {
        let raw: serde_json::Value = serde_json::from_str(source).expect("source object");
        let usage = &record.measurements.usage;
        assert_eq!(usage.raw.as_ref(), Some(&raw), "the source object is kept");
        let count = |value: Option<&serde_json::Value>| {
            value.and_then(serde_json::Value::as_u64).map(|v| v as u32)
        };
        assert_eq!(
            (
                usage.input_tokens,
                usage.output_tokens,
                usage.cache_read_tokens
            ),
            (
                count(raw.get("prompt_tokens")),
                count(raw.get("completion_tokens")),
                count(raw.pointer("/prompt_tokens_details/cached_tokens"))
            ),
            "the record's counters are its own object's"
        );
        assert_eq!(
            record.token,
            Some(TokenAnnotation::eligible(UsageFinality::Cumulative))
        );
    }
    let times: Vec<_> = usages.iter().map(|record| record.observed_at).collect();
    assert!(
        times.windows(2).all(|pair| pair[0] <= pair[1]),
        "observation times follow the order the objects were parsed"
    );
    let sample = sample.expect("one request sample");
    let maximum = |field: fn(&NormalizedUsage) -> Option<u32>| {
        usages
            .iter()
            .filter_map(|record| field(&record.measurements.usage))
            .max()
    };
    assert_eq!(
        (sample.input.value(), sample.output.value()),
        (
            maximum(|usage| usage.input_tokens),
            maximum(|usage| usage.output_tokens)
        ),
        "per-field maxima come from the reducer over the source objects"
    );
    assert_eq!(sample.usage, TokenUsageState::Incomplete);
}

#[test]
fn more_than_sixteen_finals_complete_the_job_and_leave_the_conflict_to_the_reducer() {
    let mut steps: Vec<Step> = (0..20).map(|_| Step::Chunk(FINAL_40_60)).collect();
    steps.extend((0..20u32).map(|n| {
        Step::Chunk(leaked_chunk(format!(
            "data: {{\"choices\":[],\"usage\":{{\"prompt_tokens\":4000000000,\"completion_tokens\":{}}}}}\n\n",
            2_000_000_001 + n
        )))
    }));
    steps.push(Step::Chunk(STOP_CHUNK));
    steps.push(Step::Chunk(DONE_CHUNK));
    let (sample, job, records, availability) = run_and_inspect("token-many-finals", steps, BOUND);
    assert!(
        job.completed_at.is_some() && job.unknown_at.is_none(),
        "the Job completes: a Token conflict is not an execution error"
    );
    assert_eq!(
        availability
            .iter()
            .map(|record| record.observation.classification())
            .collect::<Vec<_>>(),
        [AvailabilityClassification::Available],
        "and no availability fault is recorded"
    );
    let finals = records
        .iter()
        .filter(|record| record.token == Some(TokenAnnotation::eligible(UsageFinality::Final)))
        .count();
    assert_eq!(finals, 40, "identical and distinct finals are all stored");
    let sample = sample.expect("one request sample");
    assert_eq!(sample.input, TokenCount::Final(4_000_000_000));
    assert_eq!(sample.output, TokenCount::Conflicted);
    assert_eq!(sample.total, None);
}

#[test]
fn a_normal_finish_without_a_final_chunk_leaves_the_usage_provisional() {
    let (sample, job, _) = run_and_reduce(
        "token-no-final",
        vec![
            Step::Chunk(CUM_25),
            Step::Chunk(STOP_CHUNK),
            Step::Chunk(DONE_CHUNK),
        ],
        BOUND,
    );
    let sample = sample.expect("one request sample");
    assert!(
        job.completed_at.is_some(),
        "HTTP 200 and a normal finish settle the Job"
    );
    assert_eq!(
        (sample.input, sample.output, sample.usage, sample.total),
        (
            TokenCount::Provisional(25),
            TokenCount::Provisional(2),
            TokenUsageState::Incomplete,
            Some(27)
        ),
        "none of Completed, a finish reason, [DONE] or the end of the stream makes usage final"
    );
}

#[test]
fn an_incomplete_final_keeps_earlier_counts_provisional_and_zero_stays_known() {
    let (sample, _, _) = run_and_reduce(
        "token-partial-final",
        vec![
            Step::Chunk(CUM_25),
            Step::Chunk(STOP_CHUNK),
            Step::Chunk(FINAL_INPUT_ONLY),
        ],
        BOUND,
    );
    let sample = sample.expect("one request sample");
    assert_eq!(
        (sample.input, sample.output, sample.usage),
        (
            TokenCount::Final(40),
            TokenCount::Provisional(2),
            TokenUsageState::Incomplete
        ),
        "a final that omits output does not promote the earlier output"
    );

    let (sample, _, _) = run_and_reduce(
        "token-zero",
        vec![Step::Chunk(STOP_CHUNK), Step::Chunk(FINAL_ZERO_OUTPUT)],
        BOUND,
    );
    let sample = sample.expect("one request sample");
    assert_eq!(
        (sample.input, sample.output, sample.usage, sample.total),
        (
            TokenCount::Final(0),
            TokenCount::Final(0),
            TokenUsageState::Complete,
            Some(0)
        ),
        "a reported zero is a count"
    );
}

#[test]
fn duplicate_finals_are_idempotent_and_conflicting_finals_stay_conflicting() {
    let (sample, _, records) = run_and_reduce(
        "token-conflict",
        vec![
            Step::Chunk(FINAL_40_60),
            Step::Chunk(FINAL_40_60),
            Step::Chunk(FINAL_40_61),
            Step::Chunk(FINAL_40_60),
            Step::Chunk(STOP_CHUNK),
        ],
        BOUND,
    );
    let sample = sample.expect("one request sample");
    assert_eq!(
        records
            .iter()
            .filter(|record| record.token.is_some())
            .count(),
        5,
        "the anchor and all four reported finals are stored"
    );
    assert_eq!(
        sample.input,
        TokenCount::Final(4_000_000_000),
        "the duplicates agree"
    );
    assert_eq!(
        (sample.output, sample.total, sample.usage),
        (TokenCount::Conflicted, None, TokenUsageState::Incomplete),
        "a repeated candidate does not clear the conflict"
    );
}

/// A run that stops with usage already reported. Each stop is its own test:
/// the ntex timer belongs to the first runtime a thread creates, so a test
/// that needs the timeout runs one runtime.
fn assert_stop_keeps_reported_usage(name: &str, steps: Vec<Step>, timeout: Duration) {
    let (sample, job, _) = run_and_reduce(name, steps, timeout);
    let sample = sample.expect("an eligible request sample");
    assert!(
        job.unknown_at.is_some() && job.completed_at.is_none() && job.failed_at.is_none(),
        "{name}: the Job's outcome is unchanged: unknown"
    );
    assert_eq!(
        (sample.input, sample.output, sample.usage),
        (
            TokenCount::Provisional(25),
            TokenCount::Provisional(2),
            TokenUsageState::Incomplete
        ),
        "{name}: the usage reported before the stop is stored"
    );
}

#[test]
fn usage_reported_before_a_transport_failure_is_stored() {
    assert_stop_keeps_reported_usage(
        "token-transport-failure",
        vec![Step::Chunk(CUM_25), Step::Fail],
        BOUND,
    );
}

#[test]
fn usage_reported_before_a_stream_without_a_finish_is_stored() {
    assert_stop_keeps_reported_usage("token-no-finish", vec![Step::Chunk(CUM_25)], BOUND);
}

#[test]
fn usage_reported_before_a_cancellation_is_stored() {
    assert_stop_keeps_reported_usage(
        "token-cancelled",
        vec![Step::Chunk(CUM_25), Step::Cancel, Step::Chunk(CUM_10)],
        BOUND,
    );
}

#[test]
fn usage_reported_before_the_outer_timeout_is_stored() {
    assert_stop_keeps_reported_usage(
        "token-timeout",
        vec![Step::Chunk(CUM_25), Step::Stall],
        Duration::from_millis(150),
    );
}

/// What a driven run left durable, read after reopening the database.
struct Reopened {
    job: JobRecord,
    outputs: Vec<(OutputKind, Option<String>)>,
    /// The Metrics records carrying a reported usage object.
    usages: Vec<(Option<TokenAnnotation>, NormalizedUsage)>,
    availability: Vec<AvailabilityStatRecord>,
}

/// Drives `steps` through the real parser into the real sink, settles the
/// run, and reads what it left after reopening the database.
fn run_reopened(
    name: &str,
    steps: Vec<Step>,
    config: ProviderExecutionConfig,
    conversation: ChatConversation,
) -> Reopened {
    let (_root, directory) = data_directory(name);
    let worker = DatabaseWorker::start(&directory).expect("start database");
    let handle = worker.handle();
    let (_provider, llm) = llm_setup(&handle, "model-a");
    let run = token_run(&handle, llm);
    drive_with(&handle, run, steps, config, conversation).expect("settle the run");
    worker.shutdown().expect("shutdown");

    let worker = DatabaseWorker::start(&directory).expect("reopen database");
    let handle = worker.handle();
    let reopened = Reopened {
        job: wait(handle.job(run.job))
            .expect("read job")
            .expect("job exists"),
        outputs: wait(handle.job_outputs(run.job, None, 100))
            .expect("read outputs")
            .into_iter()
            .map(|output| (output.kind, output.summary))
            .collect(),
        usages: job_metrics(&handle, run.job)
            .into_iter()
            .filter(|record| record.measurements.usage.raw.is_some())
            .map(|record| (record.token, record.measurements.usage))
            .collect(),
        availability: wait(handle.job_availability(run.job, None, 100))
            .expect("read AvailabilityStats"),
    };
    worker.shutdown().expect("shutdown");
    assert_relationships_consistent(&directory);
    reopened
}

/// The outcome stays unknown, no result is claimed, and the trusted path
/// confirmed the local stop.
fn assert_unknown_and_stopped(case: &str, job: &JobRecord) {
    assert!(
        job.unknown_at.is_some() && job.stop_confirmed_at.is_some(),
        "{case}: the outcome is unknown and the local stop is confirmed"
    );
    assert_eq!(
        (job.completed_at, job.failed_at, job.cancelled_at),
        (None, None, None),
        "{case}: no outcome is claimed"
    );
}

const HI_CHUNK: &str = "data: {\"choices\":[{\"delta\":{\"content\":\"hi\"}}]}\n\n";

/// A conversation offering the one tool `f`.
fn offering_f() -> ChatConversation {
    ChatConversation {
        messages: vec![crate::openai_compatible::ChatMessage::user("weather?")],
        tools: vec![crate::openai_compatible::ToolWire {
            tool_type: "function".into(),
            function: crate::openai_compatible::FunctionDefinitionWire {
                name: "f".into(),
                description: None,
                parameters: Some(json!({"type": "object"})),
                strict: None,
            },
        }],
        ..ChatConversation::default()
    }
}

#[test]
fn usage_beside_a_refused_tool_call_is_stored_with_its_own_fields() {
    let usage = r#"{"prompt_tokens":7,"completion_tokens":2,"prompt_tokens_details":{"cached_tokens":1},"vendor":{"n":1}}"#;
    let refused = leaked_chunk(format!(
        "data: {{\"choices\":[{{\"delta\":{{\"content\":\"lost\",\"tool_calls\":[{{\"index\":0,\"id\":\"c\",\"type\":\"code_interpreter\"}}]}}}}],\"usage\":{usage}}}\n\n"
    ));
    let run = run_reopened(
        "refused-tool-usage",
        vec![
            Step::Chunk(HI_CHUNK),
            Step::Chunk(refused),
            Step::Chunk(STOP_CHUNK),
        ],
        ProviderExecutionConfig::default(),
        offering_f(),
    );
    assert_unknown_and_stopped("a refused tool call", &run.job);
    assert_eq!(
        run.outputs,
        [(OutputKind::Progress, Some("hi".to_owned()))],
        "the accepted text is kept and nothing of the refused event is published"
    );
    let raw: serde_json::Value = serde_json::from_str(usage).expect("usage object");
    assert_eq!(
        run.usages,
        [(
            Some(TokenAnnotation::eligible(UsageFinality::Cumulative)),
            NormalizedUsage {
                input_tokens: Some(7),
                output_tokens: Some(2),
                cache_read_tokens: Some(1),
                raw: Some(raw),
                ..NormalizedUsage::default()
            }
        )],
        "the usage beside the refused call is stored as reported"
    );
    let [record] = run.availability.as_slice() else {
        panic!("one observation, got {:?}", run.availability);
    };
    assert_eq!(
        (
            record.observation.status_code,
            record.observation.finish_reason
        ),
        (Some(200), None)
    );
    assert!(
        record
            .observation
            .failure
            .as_ref()
            .is_some_and(|failure| failure.message.contains("unsupported type"))
    );
    assert_eq!(
        record.observation.classification(),
        AvailabilityClassification::Unconfirmed
    );
}

/// Drives a response whose last event, with its usage, reaches the output
/// limit through `delta`.
fn assert_output_limit_is_a_local_stop(name: &str, delta: &str) {
    let usage = r#"{"prompt_tokens":9,"completion_tokens":3,"extra":true}"#;
    let trigger = leaked_chunk(format!(
        "data: {{\"choices\":[{{\"delta\":{delta}}}],\"usage\":{usage}}}\n\n"
    ));
    let run = run_reopened(
        name,
        vec![
            Step::Chunk(HI_CHUNK),
            Step::Chunk(trigger),
            Step::Chunk(STOP_CHUNK),
        ],
        ProviderExecutionConfig {
            max_output_bytes: 2,
            ..ProviderExecutionConfig::default()
        },
        offering_f(),
    );
    assert_unknown_and_stopped(name, &run.job);
    assert_eq!(
        run.outputs,
        [(OutputKind::Progress, Some("hi".to_owned()))],
        "{name}: the accepted prefix is kept"
    );
    let raw: serde_json::Value = serde_json::from_str(usage).expect("usage object");
    assert_eq!(
        run.usages,
        [(
            Some(TokenAnnotation::eligible(UsageFinality::Cumulative)),
            NormalizedUsage {
                input_tokens: Some(9),
                output_tokens: Some(3),
                raw: Some(raw),
                ..NormalizedUsage::default()
            }
        )],
        "{name}: the usage of the event that reached the limit is stored"
    );
    assert_eq!(
        run.availability
            .iter()
            .map(|record| record.observation.clone())
            .collect::<Vec<_>>(),
        [AvailabilityObservation {
            status_code: Some(200),
            finish_reason: None,
            raw_finish_reason: None,
            failure: None,
        }],
        "{name}: a local stop records no Provider failure and invents no finish"
    );
}

#[test]
fn text_reaching_the_output_limit_is_a_local_stop_that_keeps_its_usage() {
    assert_output_limit_is_a_local_stop("limit-text", r#"{"content":"x"}"#);
}

#[test]
fn reasoning_reaching_the_output_limit_is_a_local_stop_that_keeps_its_usage() {
    assert_output_limit_is_a_local_stop("limit-reasoning", r#"{"reasoning_content":"x"}"#);
}

#[test]
fn a_tool_call_reaching_the_output_limit_is_a_local_stop_that_keeps_its_usage() {
    assert_output_limit_is_a_local_stop("limit-tool", r#"{"tool_calls":[{"index":0,"id":"c"}]}"#);
}

#[test]
fn a_refused_tool_call_keeps_its_response_facts_and_stays_unconfirmed() {
    let no_id = "data: {\"choices\":[{\"delta\":{\"tool_calls\":[{\"index\":0,\"function\":{\"name\":\"f\",\"arguments\":\"{}\"}}]}}]}\n\n";
    let run = run_reopened(
        "refused-tool-facts",
        vec![
            Step::Chunk(no_id),
            Step::Chunk(STOP_CHUNK),
            Step::Chunk(FINAL_INPUT_ONLY),
        ],
        ProviderExecutionConfig::default(),
        offering_f(),
    );
    assert_unknown_and_stopped("a refused tool call", &run.job);
    let [record] = run.availability.as_slice() else {
        panic!("one observation, got {:?}", run.availability);
    };
    assert_eq!(
        (
            record.observation.status_code,
            record.observation.finish_reason,
            record.observation.raw_finish_reason.as_deref()
        ),
        (Some(200), Some(ChatFinishReason::Stop), Some("stop")),
        "the observed status and finish are kept"
    );
    assert!(record.observation.failure.is_some());
    assert_eq!(
        sample_of(&run.availability).classification,
        AvailabilityClassification::Unconfirmed,
        "a refused tool response is not made available by its stop finish"
    );
    assert_eq!(
        run.usages
            .iter()
            .map(|(token, _)| *token)
            .collect::<Vec<_>>(),
        [Some(TokenAnnotation::eligible(UsageFinality::Final))],
        "the final usage read before the refusal is stored"
    );
}

#[test]
fn a_transport_failure_after_a_finish_keeps_the_finish_apart_from_the_failure() {
    let run = run_reopened(
        "finish-then-reset",
        vec![Step::Chunk(HI_CHUNK), Step::Chunk(STOP_CHUNK), Step::Fail],
        ProviderExecutionConfig::default(),
        ChatConversation::default(),
    );
    assert_unknown_and_stopped("a reset after the finish", &run.job);
    assert_eq!(run.outputs, [(OutputKind::Progress, Some("hi".to_owned()))]);
    let observations: Vec<_> = run
        .availability
        .iter()
        .map(|record| record.observation.clone())
        .collect();
    assert_eq!(observations.len(), 2, "{observations:?}");
    assert_eq!(
        observations[0],
        response(200),
        "the observed finish is kept as observed, without the later failure"
    );
    assert_eq!(
        (
            observations[1].status_code,
            observations[1].finish_reason,
            observations[1].failure.is_some()
        ),
        (None, None, true),
        "the transport failure is its own observation"
    );
    assert_eq!(
        sample_of(&run.availability).classification,
        AvailabilityClassification::Available,
        "the reducer reads the witness and the status-less failure by its existing rules"
    );
}

#[test]
fn a_stop_before_any_usage_still_leaves_an_eligible_missing_usage_sample() {
    let (sample, job, records) = run_and_reduce(
        "token-timeout-no-usage",
        vec![Step::Stall],
        Duration::from_millis(150),
    );
    assert!(job.unknown_at.is_some());
    let sample = sample.expect("an eligible request sample");
    assert_eq!(sample.usage, TokenUsageState::Missing);
    assert_eq!(sample.first_metrics, records[0].id);
    assert_eq!(sample.observed_at, records[0].observed_at);
}

/// Resolves no credential: nothing reaches a network.
struct NoCredential;

impl CredentialResolver for NoCredential {
    fn resolve(
        &self,
        _: Option<&str>,
    ) -> Result<Option<String>, crate::provider_execution::ProviderExecutionError> {
        Ok(None)
    }
}

struct RefusedCredential;

impl CredentialResolver for RefusedCredential {
    fn resolve(
        &self,
        _: Option<&str>,
    ) -> Result<Option<String>, crate::provider_execution::ProviderExecutionError> {
        Err(ProviderExecutionError::Build {
            message: "credential reference is not present".into(),
        })
    }
}

/// Executes the real request path against `endpoint` and settles the Job.
fn execute_for_real(
    name: &str,
    endpoint: Option<String>,
    resolver: std::sync::Arc<dyn CredentialResolver>,
    config: ProviderExecutionConfig,
) -> (
    Option<TokenRequestSample>,
    JobRecord,
    Vec<crate::persistence::MetricsRecord>,
) {
    let (_root, directory) = data_directory(name);
    let worker = DatabaseWorker::start(&directory).expect("start database");
    let handle = worker.handle();
    let provider: LlmProviderId = wait(handle.create_config(NewConfig {
        name: "local".into(),
        body: ConfigBody::Connection(ConnectionConfig {
            protocol: OPENAI,
            endpoint,
            credential_ref: None,
        }),
    }))
    .expect("create Connection")
    .into();
    let llm = llm_profile(
        &handle,
        provider,
        llm_config("model-a", &[ModelCapability::Chat]),
    );
    let run = token_run(&handle, llm);
    let frozen = wait(handle.job_configuration(run.job))
        .expect("read configuration")
        .expect("configured")
        .frozen;
    let execution = {
        let handle = handle.clone();
        on_ntex(async move {
            let mut sink = output_sink(handle.clone(), run.job);
            let execution =
                execute_chat_with_sink(&frozen, Some("hi"), resolver.as_ref(), &config, &mut sink)
                    .await;
            finish_provider_job(handle, run.executor, run.input, run.job, sink, execution).await
        })
    };
    execution.expect("settle the run");
    let records = job_metrics(&handle, run.job);
    let job = wait(handle.job(run.job))
        .expect("read job")
        .expect("job exists");
    let sample = TokenRequestSample::from_metrics(&records).expect("one Job");
    worker.shutdown().expect("shutdown");
    (sample, job, records)
}

#[test]
fn a_request_known_not_to_have_been_sent_is_excluded_whatever_the_job_outcome() {
    let cancelled = ProviderExecutionConfig::default();
    cancelled.cancellation.cancel();
    let cases = [
        (
            "token-build-failure",
            std::sync::Arc::new(RefusedCredential) as std::sync::Arc<dyn CredentialResolver>,
            ProviderExecutionConfig::default(),
        ),
        (
            "token-cancelled-before-send",
            std::sync::Arc::new(NoCredential) as std::sync::Arc<dyn CredentialResolver>,
            cancelled,
        ),
    ];
    for (name, resolver, config) in cases {
        let (sample, job, records) = execute_for_real(
            name,
            Some("https://127.0.0.1:9/v1".into()),
            resolver,
            config,
        );
        assert!(job.failed_at.is_some(), "{name}: the Job fails");
        assert_eq!(sample, None, "{name}: no Token request sample");
        assert_eq!(
            records
                .iter()
                .filter_map(|record| record.token)
                .collect::<Vec<_>>(),
            [TokenAnnotation::excluded()],
            "{name}: the exclusion is recorded and is not an eligible anchor"
        );
    }
}

#[test]
fn a_send_that_may_have_been_delivered_is_eligible_even_though_nothing_answered() {
    // A listener that accepts connections and never answers.
    let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind a local port");
    let port = listener.local_addr().expect("local address").port();
    listener.set_nonblocking(true).expect("non-blocking");
    let stop = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
    let holder = {
        let stop = stop.clone();
        std::thread::spawn(move || {
            let mut held = Vec::new();
            while !stop.load(std::sync::atomic::Ordering::SeqCst) {
                match listener.accept() {
                    Ok((stream, _)) => held.push(stream),
                    Err(_) => std::thread::sleep(Duration::from_millis(10)),
                }
            }
        })
    };
    let (sample, job, records) = execute_for_real(
        "token-unanswered-send",
        Some(format!("https://127.0.0.1:{port}/v1")),
        std::sync::Arc::new(NoCredential),
        ProviderExecutionConfig {
            timeout: Duration::from_millis(300),
            ..ProviderExecutionConfig::default()
        },
    );
    stop.store(true, std::sync::atomic::Ordering::SeqCst);
    holder.join().expect("listener thread");
    assert!(
        job.unknown_at.is_some(),
        "an unanswered send is an unknown outcome"
    );
    assert_eq!(
        sample.map(|sample| sample.usage),
        Some(TokenUsageState::Missing),
        "delivery cannot be ruled out, so the request is eligible with missing usage"
    );
    assert_eq!(
        records
            .iter()
            .filter_map(|record| record.token)
            .collect::<Vec<_>>(),
        [TokenAnnotation::eligible(UsageFinality::Cumulative)]
    );
}

#[test]
fn a_rejection_after_a_response_head_is_eligible_though_the_job_fails() {
    let (_root, directory) = data_directory("token-rejection");
    let worker = DatabaseWorker::start(&directory).expect("start database");
    let handle = worker.handle();
    let (_provider, llm) = llm_setup(&handle, "model-a");
    let run = token_run(&handle, llm);
    let failure = ProviderFailure {
        message: "rate limited".into(),
        status_code: Some(429),
        ..ProviderFailure::default()
    };
    let settled = {
        let handle = handle.clone();
        on_ntex(async move {
            let mut sink = output_sink(handle.clone(), run.job);
            // What the request path does when the head of any response arrives.
            announce(Some(&mut sink), ProviderFact::Delivered).await;
            finish_provider_job(
                handle,
                run.executor,
                run.input,
                run.job,
                sink,
                Err(ProviderExecutionError::Rejected { failure }),
            )
            .await
        })
    };
    settled.expect("settle the rejection");
    let job = wait(handle.job(run.job))
        .expect("read job")
        .expect("job exists");
    assert!(job.failed_at.is_some());
    assert_eq!(
        stored_sample(&handle, run.job).map(|sample| sample.usage),
        Some(TokenUsageState::Missing)
    );
    worker.shutdown().expect("shutdown");
}

#[test]
fn a_failed_observation_write_is_local_and_changes_nothing_that_settled() {
    let (_root, directory) = data_directory("token-write-failure");
    let worker = DatabaseWorker::start(&directory).expect("start database");
    let handle = worker.handle();
    let (_provider, llm) = llm_setup(&handle, "model-a");
    let run = token_run(&handle, llm);
    let missing_job = JobId::new(9_999_999);
    let receipt = {
        let handle = handle.clone();
        on_ntex(async move {
            let config = ProviderExecutionConfig::default();
            // Observations go to a Job that does not exist, so each write is refused.
            let mut sink = output_sink(handle.clone(), missing_job);
            announce(Some(&mut sink), ProviderFact::Delivered).await;
            let execution = read_response(
                payload(
                    vec![Step::Chunk(STOP_CHUNK), Step::Chunk(FINAL_INPUT_ONLY)],
                    config.cancellation.clone(),
                ),
                200,
                Instant::now(),
                None,
                &config,
                Some(&mut sink),
                &ChatConversation::default(),
            )
            .await;
            finish_provider_job(handle, run.executor, run.input, run.job, sink, execution).await
        })
    };
    match receipt {
        Err(ExecutionRuntimeError::ObservationsNotRecorded { receipt, source }) => {
            assert_eq!(receipt.job, run.job);
            assert!(
                matches!(source, DatabaseError::Rejected(_)),
                "a storage rule refused the write, not a Provider: {source:?}"
            );
        }
        other => panic!("expected the settled-but-unrecorded error, got {other:?}"),
    }
    let job = wait(handle.job(run.job))
        .expect("read job")
        .expect("job exists");
    assert!(
        job.completed_at.is_some() && job.unknown_at.is_none(),
        "the authoritative settlement stands"
    );
    assert_eq!(
        wait(handle.job_availability(run.job, None, 10))
            .expect("read AvailabilityStats")
            .len(),
        1,
        "the availability observation is still recorded"
    );
    assert!(
        job_metrics(&handle, run.job)
            .iter()
            .all(|record| record.token.is_none()),
        "no Token fact was invented for the Job"
    );
    worker.shutdown().expect("shutdown");

    // A settled failure whose availability write is refused keeps its outcome too.
    let (_root, directory) = data_directory("token-availability-refused");
    let worker = DatabaseWorker::start(&directory).expect("start database");
    let handle = worker.handle();
    let plain = create_job(
        &handle,
        new_job(&handle, "plain", handle.genesis(), Vec::new()),
    );
    wait(handle.claim_fixture(plain, test_executor(&handle))).expect("start job");
    let input = wait(handle.job(plain))
        .expect("read job")
        .expect("job exists")
        .input;
    let executor = executor(&handle, "llm worker", &[OPENAI]).expect("register");
    wait(handle.record_availability_stat(NewAvailabilityStat {
        job: plain,
        request: RequestNumber::FIRST,
        observed_at: Timestamp::now(),
        observation: response(200),
    }))
    .expect("original response status");
    let failure = ProviderFailure {
        message: "bad".into(),
        status_code: Some(400),
        ..ProviderFailure::default()
    };
    match wait(settle_provider_error(
        handle.clone(),
        executor,
        input,
        plain,
        RequestNumber::FIRST,
        ProviderExecutionError::Rejected { failure },
    )) {
        Ok(receipt) => assert_eq!(receipt.job, plain),
        other => panic!("expected the settled-but-unrecorded error, got {other:?}"),
    }
    assert!(
        wait(handle.job(plain))
            .expect("read job")
            .expect("job exists")
            .failed_at
            .is_some(),
        "the Job stays failed"
    );
    worker.shutdown().expect("shutdown");
}

mod control_plane {
    use std::collections::BTreeMap;
    use std::io::{Read, Write};
    use std::net::{SocketAddr, TcpListener, TcpStream};
    use std::sync::{Arc, Mutex};

    use serde_json::Value;

    use super::*;
    use crate::control_plane::ControlPlaneConfig;
    use crate::host::{HostContext, Phase, run_controlled_reporting};
    use crate::persistence::{IntermediateOutputKind, JobFinalization, NewJobOutput};

    const TOKEN: &str = "T0k3n-For-The-Control-Plane_Tests.0123456789~abc";

    fn config() -> ControlPlaneConfig {
        ControlPlaneConfig::new(0, TOKEN).expect("a valid configuration")
    }

    /// One HTTP/1.1 response: its status, headers (lowercase names) and body.
    struct Response {
        status: u16,
        headers: BTreeMap<String, String>,
        body: String,
    }

    impl Response {
        fn json(&self) -> Value {
            serde_json::from_str(&self.body).expect("a JSON body")
        }

        fn error(&self) -> String {
            self.json()["error"]["code"]
                .as_str()
                .expect("an error code")
                .to_owned()
        }
    }

    /// Sends `GET path` with exactly `headers`, over a new connection, and
    /// reads the whole response.
    fn send(address: SocketAddr, request_line: &str, headers: &[(&str, &str)]) -> Response {
        let mut stream = TcpStream::connect(address).expect("connect");
        stream
            .set_read_timeout(Some(crate::test_support::WAIT))
            .expect("timeout");
        let mut request = format!("{request_line} HTTP/1.1\r\n");
        for (name, value) in headers {
            request.push_str(&format!("{name}: {value}\r\n"));
        }
        request.push_str("Connection: close\r\n\r\n");
        stream.write_all(request.as_bytes()).expect("send");
        let mut raw = Vec::new();
        stream.read_to_end(&mut raw).expect("read");
        let raw = String::from_utf8(raw).expect("UTF-8");
        let (head, body) = raw.split_once("\r\n\r\n").expect("a head");
        let mut lines = head.lines();
        let status = lines
            .next()
            .and_then(|line| line.split(' ').nth(1))
            .and_then(|code| code.parse().ok())
            .expect("a status");
        let headers = lines
            .filter_map(|line| line.split_once(": "))
            .map(|(name, value)| (name.to_ascii_lowercase(), value.to_owned()))
            .collect();
        Response {
            status,
            headers,
            body: body.to_owned(),
        }
    }

    /// An authenticated request from a well-behaved local client.
    fn get(address: SocketAddr, path: &str) -> Response {
        let host = format!("127.0.0.1:{}", address.port());
        let bearer = format!("Bearer {TOKEN}");
        send(
            address,
            &format!("GET {path}"),
            &[("Host", &host), ("Authorization", &bearer)],
        )
    }

    /// Runs the host with the Control Plane, runs `client` on a thread of
    /// its own against the bound address, then stops the host. Returns the
    /// host's report and what `client` returned.
    fn hosted<T: Send + 'static>(
        directory: &DataDirectory,
        control: Option<ControlPlaneConfig>,
        client: impl FnOnce(Option<SocketAddr>) -> T + Send + 'static,
    ) -> (crate::host::HostReport, Option<T>) {
        let returned = Arc::new(Mutex::new(None));
        let slot = returned.clone();
        let report = std::thread::scope(|scope| {
            scope
                .spawn(move || {
                    run_controlled_reporting(
                        directory,
                        false,
                        control,
                        move |context: HostContext| {
                            let address = context.control_plane();
                            async move {
                                let (returned, response) = flume::bounded(1);
                                std::thread::spawn(move || returned.send(client(address)));
                                let value = response
                                    .recv_async()
                                    .await
                                    .expect("the client did not panic");
                                *slot.lock().expect("lock") = Some(value);
                            }
                        },
                    )
                })
                .join()
                .expect("the host did not panic")
        });
        // The process-wide logger may already be another host's in this
        // test binary; that is reported, and nothing else may fail.
        let returned = returned.lock().expect("lock").take();
        (report, returned)
    }

    fn only_logger_failures(report: &crate::host::HostReport) {
        assert!(
            report
                .failures
                .iter()
                .all(|failure| failure.phase == Phase::Logger),
            "{:?}",
            report.failures
        );
    }

    /// Opens the database as the host does, runs `seed`, and closes it.
    fn seeded<T>(directory: &DataDirectory, seed: impl FnOnce(&DatabaseHandle) -> T) -> T {
        let policy =
            crate::system_policy::SystemPolicy::load_for_startup(directory).expect("policy");
        let worker = DatabaseWorker::start_with_policy(directory, policy).expect("start database");
        let seeded = seed(&worker.handle());
        worker.shutdown().expect("shutdown");
        seeded
    }

    fn progress(handle: &DatabaseHandle, job: JobId, text: &str) {
        wait(handle.append_job_output(
            job,
            NewJobOutput {
                kind: IntermediateOutputKind::Progress,
                summary: Some(text.to_owned()),
                code: None,
                goal_reached: None,
                value: None,
            },
        ))
        .expect("append an output");
    }

    #[test]
    fn a_local_client_reads_each_job_as_it_was_recorded() {
        let (_root, directory) = data_directory("control-jobs");
        let jobs = seeded(&directory, |handle| {
            let (_provider, llm) = llm_setup(handle, "model-a");
            let submitted = llm_job(handle, llm);
            let running = llm_job(handle, llm);
            wait(handle.claim_fixture(running, test_executor(handle))).expect("start");
            let mut settled = Vec::new();
            for finalization in [
                JobFinalization::Completed(result("done")),
                JobFinalization::Failed(result("failed")),
                JobFinalization::Cancelled(result("cancelled")),
            ] {
                let job = llm_job(handle, llm);
                wait(handle.claim_fixture(job, test_executor(handle))).expect("start");
                wait(handle.finalize_job(job, finalization)).expect("settle");
                settled.push(job);
            }
            // Unknown, its local execution confirmed stopped.
            let confirmed = llm_job(handle, llm);
            wait(handle.claim_fixture(confirmed, test_executor(handle))).expect("start");
            progress(handle, confirmed, "partial answer");
            wait(handle.finalize_job(confirmed, JobFinalization::Unknown)).expect("unknown");
            let _ = wait(handle.confirm_job_stopped(confirmed));
            // Unknown, its stop never confirmed.
            let unconfirmed = llm_job(handle, llm);
            wait(handle.claim_fixture(unconfirmed, test_executor(handle))).expect("start");
            wait(handle.finalize_job(unconfirmed, JobFinalization::Unknown)).expect("unknown");
            [
                submitted,
                running,
                settled[0],
                settled[1],
                settled[2],
                confirmed,
                unconfirmed,
            ]
            .map(|job| wait(handle.job(job)).expect("read").expect("a Job"))
        });
        let expected = jobs.clone();
        let (report, checked) = hosted(&directory, Some(config()), move |address| {
            let address = address.expect("the control plane listens");
            assert!(address.ip().is_loopback());
            let health = get(address, "/api/v1/health");
            assert_eq!(health.status, 200);
            assert_eq!(
                health.json(),
                json!({
                    "api_version": 1,
                    "host": "running",
                    "workspace_root": null,
                    "database": {"status": "open", "accepting_commands": true, "readable": true},
                    "quarantined": [],
                })
            );
            let states = [
                "cancelled",
                "unknown",
                "completed",
                "failed",
                "cancelled",
                "unknown",
                "unknown",
            ];
            for (record, state) in expected.iter().zip(states) {
                let response = get(address, &format!("/api/v1/jobs/{}", record.id));
                assert_eq!(response.status, 200, "{}", response.body);
                assert_eq!(
                    response.headers.get("cache-control").map(String::as_str),
                    Some("no-store")
                );
                let job = &response.json()["job"];
                assert_eq!(job["id"], record.id.to_string());
                assert_eq!(job["input"], record.input.to_string());
                assert_eq!(job["settlement"]["state"], state, "{}", record.id);
                let settled_at = if record.id == expected[0].id || record.id == expected[1].id {
                    job["settlement"]["settled_at_us"].as_i64()
                } else {
                    [
                        record.completed_at,
                        record.failed_at,
                        record.cancelled_at,
                        record.unknown_at,
                    ]
                    .into_iter()
                    .flatten()
                    .next()
                    .map(|at| at.as_unix_micros())
                };
                assert_eq!(job["settlement"]["settled_at_us"], json!(settled_at));
                assert_eq!(
                    job["started_at_us"],
                    json!(record.started_at.map(|at| at.as_unix_micros()))
                );
                let stop_confirmed_at = if record.id == expected[1].id {
                    job["stop_confirmed_at_us"].as_i64()
                } else {
                    record.stop_confirmed_at.map(|at| at.as_unix_micros())
                };
                assert_eq!(job["stop_confirmed_at_us"], json!(stop_confirmed_at));
                assert_eq!(job["created_at_us"], record.created_at.as_unix_micros());
            }
            // The Unknown Job's stop is confirmed or not, as recorded, and
            // it has no final Result: only the progress it recorded.
            let confirmed = &expected[5];
            assert!(confirmed.stop_confirmed_at.is_some());
            let outputs = get(address, &format!("/api/v1/jobs/{}/outputs", confirmed.id)).json();
            let kinds = outputs["outputs"]
                .as_array()
                .expect("outputs")
                .iter()
                .map(|output| output["kind"].as_str().expect("a kind").to_owned())
                .collect::<Vec<_>>();
            assert_eq!(kinds, ["progress"]);
            assert_eq!(outputs["outputs"][0]["summary"], "partial answer");
            // A completed Job's final Result is its own stored output.
            let completed =
                get(address, &format!("/api/v1/jobs/{}/outputs", expected[2].id)).json();
            assert_eq!(completed["outputs"][0]["kind"], "result");
            assert_eq!(completed["outputs"][0]["summary"], "done");

            for (path, status, code) in [
                ("/api/v1/jobs/job:999999", 404, "job_not_found"),
                ("/api/v1/jobs/job:999999/outputs", 404, "job_not_found"),
                ("/api/v1/jobs/7", 400, "invalid_request"),
                ("/api/v1/jobs/job:01", 400, "invalid_request"),
                ("/api/v1/jobs/job%3A1", 400, "invalid_request"),
                ("/api/v1/jobs/job:1?x=1", 400, "invalid_request"),
                ("/api/v1/jobs/job:1/", 404, "not_found"),
                ("/api/v1/jobs", 404, "not_found"),
                ("/", 404, "not_found"),
                ("/ocg_db/ocg.redb", 404, "not_found"),
                ("/api/v1/../../ocg.key", 404, "not_found"),
            ] {
                let response = get(address, path);
                assert_eq!(
                    (response.status, response.error().as_str()),
                    (status, code),
                    "{path}"
                );
            }
        });
        only_logger_failures(&report);
        assert!(checked.is_some(), "the client ran");
        let position = |phase| {
            report
                .attempted
                .iter()
                .position(|attempted| *attempted == phase)
        };
        assert!(
            position(Phase::ControlPlane) < position(Phase::ExecutionRuntime),
            "the listener stops before the execution runtime: {:?}",
            report.attempted
        );
        assert_eq!(report.attempted.last(), Some(&Phase::Database));
        // Reads changed nothing.
        let after = seeded(&directory, |handle| {
            jobs.clone()
                .map(|job| wait(handle.job(job.id)).expect("read").expect("a Job"))
        });
        // The Job that was running when the file closed was settled as
        // unknown by the opening that hosted the client; reads touched no other.
        for (position, (before, after)) in jobs.iter().zip(after.iter()).enumerate() {
            if before.any_terminal() && position != 1 {
                assert_eq!(before.updated_at, after.updated_at, "{}", before.id);
            }
        }
    }

    #[test]
    fn requests_without_the_token_or_from_another_origin_are_refused() {
        let (_root, directory) = data_directory("control-boundary");
        let (report, checked) = hosted(&directory, Some(config()), move |address| {
            let address = address.expect("listening");
            let port = address.port();
            let host = format!("127.0.0.1:{port}");
            let localhost = format!("localhost:{port}");
            let bearer = format!("Bearer {TOKEN}");
            let health = "GET /api/v1/health";
            let origin = format!("http://127.0.0.1:{port}");
            let localhost_origin = format!("http://localhost:{port}");
            let mixed = format!("Localhost:{port}");
            let mixed_origin = format!("HTTP://LOCALHOST:{port}");
            let mut responses = Vec::new();
            let mut refused = |status: u16, code: &str, request: &str, headers: &[(&str, &str)]| {
                let response = send(address, request, headers);
                assert_eq!(
                    (response.status, response.error().as_str()),
                    (status, code),
                    "{headers:?}"
                );
                responses.push(response);
            };
            // Authentication.
            refused(401, "unauthenticated", health, &[("Host", &host)]);
            refused(
                401,
                "unauthenticated",
                health,
                &[
                    ("Host", &host),
                    (
                        "Authorization",
                        "Bearer wrong-token-of-the-right-shape-0123456789",
                    ),
                ],
            );
            refused(
                401,
                "unauthenticated",
                health,
                &[("Host", &host), ("Authorization", TOKEN)],
            );
            refused(
                401,
                "unauthenticated",
                health,
                &[
                    ("Host", &host),
                    ("Authorization", &format!("Basic {TOKEN}")),
                ],
            );
            refused(
                401,
                "unauthenticated",
                health,
                &[
                    ("Host", &host),
                    ("Authorization", &format!("Bearer {}", &TOKEN[1..])),
                ],
            );
            refused(
                401,
                "unauthenticated",
                health,
                &[
                    ("Host", &host),
                    ("Authorization", &bearer),
                    ("Authorization", &bearer),
                ],
            );
            refused(
                401,
                "unauthenticated",
                &format!("GET /api/v1/health?access_token={TOKEN}"),
                &[("Host", &host)],
            );
            refused(
                400,
                "invalid_request",
                &format!("GET /api/v1/health?access_token={TOKEN}"),
                &[("Host", &host), ("Authorization", &bearer)],
            );
            // Host: the listener's own name and port only, even with the token.
            for hostile in [
                "evil.example".to_owned(),
                format!("evil.example:{port}"),
                "127.0.0.1".to_owned(),
                format!("127.0.0.1:{}", port.wrapping_add(1)),
                format!("0.0.0.0:{port}"),
                format!("[::1]:{port}"),
                format!("127.0.0.1:{port}.evil.example"),
            ] {
                refused(
                    403,
                    "forbidden_host",
                    health,
                    &[("Host", &hostile), ("Authorization", &bearer)],
                );
            }
            refused(
                403,
                "forbidden_host",
                health,
                &[
                    ("Host", &host),
                    ("Host", "evil.example"),
                    ("Authorization", &bearer),
                ],
            );
            // Origin and fetch metadata.
            for hostile in [
                "http://evil.example",
                "null",
                "https://127.0.0.1",
                &format!("https://127.0.0.1:{port}"),
                &format!("http://127.0.0.1:{port}.evil.example"),
            ] {
                refused(
                    403,
                    "forbidden_origin",
                    health,
                    &[
                        ("Host", &host),
                        ("Origin", hostile),
                        ("Authorization", &bearer),
                    ],
                );
            }
            refused(
                403,
                "forbidden_origin",
                health,
                &[
                    ("Host", &host),
                    ("Sec-Fetch-Site", "cross-site"),
                    ("Authorization", &bearer),
                ],
            );
            refused(
                403,
                "forbidden_origin",
                health,
                &[
                    ("Host", &host),
                    ("Sec-Fetch-Site", "same-site"),
                    ("Authorization", &bearer),
                ],
            );
            // A loopback Origin must name the Host's own loopback host and
            // port: another allowed loopback name, or another port, is foreign.
            let other_port = port.wrapping_add(1);
            for (host, origin) in [
                (&host, localhost_origin.clone()),
                (&localhost, origin.clone()),
                (&mixed, origin.clone()),
                (&host, format!("HTTP://LOCALHOST:{port}")),
                (&localhost, format!("http://localhost:{other_port}")),
            ] {
                refused(
                    403,
                    "forbidden_origin",
                    health,
                    &[
                        ("Host", host),
                        ("Origin", &origin),
                        ("Authorization", &bearer),
                    ],
                );
            }
            // One Origin only, even when it matches.
            refused(
                403,
                "forbidden_origin",
                health,
                &[
                    ("Host", &host),
                    ("Origin", &origin),
                    ("Origin", &origin),
                    ("Authorization", &bearer),
                ],
            );
            // Only GET; no CORS preflight is answered.
            for method in ["POST", "OPTIONS", "DELETE", "PUT"] {
                refused(
                    405,
                    "method_not_allowed",
                    &format!("{method} /api/v1/health"),
                    &[
                        ("Host", &host),
                        ("Authorization", &bearer),
                        ("Origin", "http://evil.example"),
                        ("Access-Control-Request-Method", "GET"),
                    ],
                );
            }
            // Oversized requests.
            refused(
                414,
                "uri_too_long",
                &format!("GET /api/v1/{}", "a".repeat(300)),
                &[("Host", &host), ("Authorization", &bearer)],
            );
            let big = "x".repeat(9 * 1024);
            refused(
                431,
                "headers_too_large",
                health,
                &[
                    ("Host", &host),
                    ("Authorization", &bearer),
                    ("X-Padding", &big),
                ],
            );

            // The same request from the listener's own origin is answered:
            // each loopback name with its own origin, Host matched case-blind.
            for (host, origin) in [
                (&host, Some(origin.as_str())),
                (&localhost, Some(localhost_origin.as_str())),
                (&mixed, Some(mixed_origin.as_str())),
                (&localhost, None),
            ] {
                let mut headers = vec![
                    ("Host", host.as_str()),
                    ("Authorization", bearer.as_str()),
                    ("Sec-Fetch-Site", "same-origin"),
                ];
                if let Some(origin) = origin {
                    headers.push(("Origin", origin));
                }
                let response = send(address, health, &headers);
                assert_eq!(response.status, 200, "{}", response.body);
                responses.push(response);
            }
            for response in &responses {
                assert!(
                    !response.body.contains(TOKEN),
                    "the token is never reflected"
                );
                assert!(!response.headers.values().any(|value| value.contains(TOKEN)));
                assert!(
                    !response
                        .headers
                        .keys()
                        .any(|name| name.starts_with("access-control-")),
                    "no CORS header: {:?}",
                    response.headers
                );
                assert!(!response.headers.contains_key("set-cookie"));
            }
            assert_eq!(
                responses[0]
                    .headers
                    .get("www-authenticate")
                    .map(String::as_str),
                Some("Bearer")
            );
        });
        only_logger_failures(&report);
        assert!(checked.is_some());
        // Nothing the run logged repeats the token.
        if let Ok(entries) = std::fs::read_dir(directory.logs_path()) {
            for entry in entries.flatten() {
                let text = std::fs::read_to_string(entry.path()).unwrap_or_default();
                assert!(!text.contains(TOKEN), "{}", entry.path().display());
            }
        }
    }

    #[test]
    fn outputs_are_paged_in_their_stored_sequence() {
        let (_root, directory) = data_directory("control-outputs");
        let (job, empty, large) = seeded(&directory, |handle| {
            let (_provider, llm) = llm_setup(handle, "model-a");
            let job = llm_job(handle, llm);
            wait(handle.claim_fixture(job, test_executor(handle))).expect("start");
            for index in 0..7 {
                progress(handle, job, &format!("chunk {index} \"quoted\" ünïcødé\n"));
            }
            wait(handle.finalize_job(job, JobFinalization::Completed(result("final answer"))))
                .expect("complete");
            let empty = llm_job(handle, llm);
            let large = llm_job(handle, llm);
            wait(handle.claim_fixture(large, test_executor(handle))).expect("start");
            progress(handle, large, "small");
            progress(handle, large, &"L".repeat(4_000));
            (job, empty, large)
        });
        let stored = seeded(&directory, move |handle| {
            wait(handle.job_outputs(job, None, 1_000)).expect("read outputs")
        });
        assert_eq!(stored.len(), 8);
        let (report, checked) = hosted(&directory, Some(config()), move |address| {
            let address = address.expect("listening");
            let page = |query: &str| {
                let response = get(address, &format!("/api/v1/jobs/{job}/outputs{query}"));
                assert_eq!(response.status, 200, "{}", response.body);
                response.json()
            };
            // Every output, page by page, exactly once and in order.
            let mut seen = Vec::new();
            let mut cursor: Option<i64> = None;
            loop {
                let query = match cursor {
                    None => "?limit=3".to_owned(),
                    Some(after) => format!("?after_seq={after}&limit=3"),
                };
                let body = page(&query);
                assert_eq!(body["after_seq"], json!(cursor));
                let outputs = body["outputs"].as_array().expect("outputs").clone();
                assert!(outputs.len() <= 3);
                seen.extend(outputs);
                assert_eq!(
                    body["next_after_seq"],
                    seen.last()
                        .map_or(json!(cursor), |last| last["seq"].clone())
                );
                cursor = body["next_after_seq"].as_i64();
                if body["has_more"] == false {
                    break;
                }
            }
            assert_eq!(seen.len(), stored.len());
            for (shown, record) in seen.iter().zip(&stored) {
                assert_eq!(shown["seq"], record.seq);
                assert_eq!(shown["id"], record.id.to_string());
                assert_eq!(
                    shown["summary"],
                    json!(record.summary),
                    "stored content, exactly"
                );
                assert_eq!(shown["created_at_us"], record.created_at.as_unix_micros());
            }
            assert_eq!(seen.last().expect("an output")["kind"], "result");
            // A cursor at the last output is an empty tail.
            let last = stored.last().expect("an output").seq;
            let tail = page(&format!("?after_seq={last}"));
            assert_eq!(tail["outputs"], json!([]));
            assert_eq!(
                (&tail["next_after_seq"], &tail["has_more"]),
                (&json!(last), &json!(false))
            );
            // A cursor in the middle starts strictly after it.
            let middle = stored[3].seq;
            let rest = page(&format!("?after_seq={middle}"));
            assert_eq!(rest["outputs"][0]["seq"], stored[4].seq);
            assert_eq!(rest["outputs"].as_array().expect("outputs").len(), 4);
            // A Job with no output.
            let none = get(address, &format!("/api/v1/jobs/{empty}/outputs")).json();
            assert_eq!(
                (&none["outputs"], &none["next_after_seq"], &none["has_more"]),
                (&json!([]), &Value::Null, &json!(false))
            );
            for query in [
                "?limit=0",
                "?limit=101",
                "?after_seq=-1",
                "?after_seq=a",
                "?limit=2&limit=3",
                "?cursor=1",
            ] {
                let response = get(address, &format!("/api/v1/jobs/{job}/outputs{query}"));
                assert_eq!(
                    (response.status, response.error().as_str()),
                    (400, "invalid_request"),
                    "{query}"
                );
            }
            let large_page = get(address, &format!("/api/v1/jobs/{large}/outputs"));
            assert_eq!(large_page.status, 200);
            assert_eq!(
                large_page.json()["outputs"][1]["summary"],
                "L".repeat(4_000)
            );
        });
        only_logger_failures(&report);
        assert!(checked.is_some());

        // With a smaller response limit, a page stops before an output that
        // does not fit, and an output that alone does not fit is refused,
        // never cut.
        let small = config().with_response_bytes(1_024);
        let (report, checked) = hosted(&directory, Some(small), move |address| {
            let address = address.expect("listening");
            let first = get(address, &format!("/api/v1/jobs/{large}/outputs"));
            assert_eq!(first.status, 200);
            let body = first.json();
            let outputs = body["outputs"].as_array().expect("outputs");
            assert_eq!(outputs.len(), 1);
            assert_eq!(outputs[0]["summary"], "small");
            assert_eq!(body["has_more"], true);
            assert!(first.body.len() <= 1_024);
            let next = body["next_after_seq"].as_i64().expect("a cursor");
            let refused = get(
                address,
                &format!("/api/v1/jobs/{large}/outputs?after_seq={next}"),
            );
            assert_eq!(
                (refused.status, refused.error().as_str()),
                (422, "output_too_large")
            );
            assert!(!refused.body.contains("LLLL"), "no part of it is sent");
        });
        only_logger_failures(&report);
        assert!(checked.is_some());
    }

    #[test]
    fn the_control_plane_runs_only_when_configured_and_releases_its_port() {
        let (_root, directory) = data_directory("control-lifecycle");
        // Off by default: nothing listens, and the run is as it was.
        let (report, address) = hosted(&directory, None, |address| address);
        only_logger_failures(&report);
        assert_eq!(address, Some(None));
        assert!(!report.attempted.contains(&Phase::ControlPlane));

        // A port that cannot be bound fails the run before its caller runs.
        let occupied = TcpListener::bind("127.0.0.1:0").expect("occupy a port");
        let port = occupied.local_addr().expect("address").port();
        let busy = ControlPlaneConfig::new(port, TOKEN).expect("valid");
        let (report, ran) = hosted(&directory, Some(busy), |_| ());
        assert!(ran.is_none(), "the caller is not run without its listener");
        let failures = report
            .failures
            .iter()
            .filter(|failure| failure.phase == Phase::ControlPlane)
            .collect::<Vec<_>>();
        let [failure] = failures.as_slice() else {
            panic!("one control plane failure: {:?}", report.failures);
        };
        assert!(failure.error.contains(&format!("127.0.0.1:{port}")));
        assert!(!failure.error.contains(TOKEN));
        assert_eq!(
            report.attempted.last(),
            Some(&Phase::Database),
            "the host still stopped"
        );
        drop(occupied);

        // Once the host stopped, its port is free again.
        let (report, address) = hosted(&directory, Some(config()), |address| {
            let address = address.expect("listening");
            assert_eq!(get(address, "/api/v1/health").status, 200);
            address
        });
        only_logger_failures(&report);
        let address = address.expect("ran");
        TcpListener::bind(address).expect("the listener's port was released");
        assert!(
            TcpStream::connect(address).is_err(),
            "nothing answers there"
        );
    }
}

// ---- runtime lifecycle closure -----------------------------------------------------

/// Local execution claims, stop confirmation, abandoned-work settlement and
/// bounded runtime shutdown.
mod lifecycle_closure {
    use std::net::{TcpListener, TcpStream};
    use std::sync::Arc;

    use super::*;
    use crate::disk_protection::{
        CleanupConfig, FilesystemConfig, ProtectedArea, ProtectionReason, SpaceShortage,
    };
    use crate::orchestration::execution_runtime::ProviderExecutionPlan;
    use crate::persistence::DependencyCondition::{Settled, Success};
    use crate::persistence::DependencyState::{Impossible, Waiting};
    use crate::provider_execution::{
        CancellationToken, EnvironmentCredentialResolver, ProviderExecutionConfig,
    };

    fn record(handle: &DatabaseHandle, job: JobId) -> JobRecord {
        wait(handle.job(job))
            .expect("read job")
            .expect("job exists")
    }

    /// A configured Job that this process started as its own execution.
    fn local_child(handle: &DatabaseHandle, agent: ProfileId, llm: ProfileId) -> JobId {
        let job = configured_child(handle, agent, llm);
        wait(handle.claim_fixture(job, test_executor(handle))).expect("start the Job locally");
        job
    }

    fn space_only(directory: &DataDirectory) -> ProtectionReason {
        ProtectionReason::InsufficientSpace(SpaceShortage {
            area: ProtectedArea::Database,
            path: directory.database_path().to_path_buf(),
            available_bytes: 0,
            expected_write_bytes: 0,
            threshold_bytes: 1,
        })
    }

    /// Thresholds any test machine meets, and no periodic check of its own
    /// during a test.
    fn lenient() -> FilesystemConfig {
        FilesystemConfig {
            disk_lock_threshold_bytes: 1,
            disk_resume_threshold_bytes: 2,
            disk_check_interval_ms: 3_600_000,
            cleanup_config: CleanupConfig::default(),
        }
    }

    #[test]
    fn every_unknown_provider_settlement_confirms_the_local_stop_and_releases_its_profiles() {
        let (_root, directory) = data_directory("unknown-stops");
        let worker = DatabaseWorker::start(&directory).expect("start database");
        let handle = worker.handle();
        let Configured {
            targeted, agent, ..
        } = configured(&handle);
        let executor = executor(&handle, "llm worker", &[OPENAI]).expect("register Executor");
        let active = local_child(&handle, agent, targeted);
        let facts = ResponseFacts {
            status_code: 200,
            finish_reason: None,
            raw_finish_reason: None,
        };
        let endings = [
            ProviderExecutionError::Unknown {
                message: "provider request timed out after it may have been delivered".into(),
                facts: None,
                finished: false,
            },
            ProviderExecutionError::Unknown {
                message: "connection reset".into(),
                facts: Some(facts.clone()),
                finished: false,
            },
            ProviderExecutionError::Incomplete {
                message: "provider stream ended without a finish event".into(),
                facts: facts.clone(),
            },
            ProviderExecutionError::Stopped {
                message: "provider output exceeded its limit".into(),
                facts: facts.clone(),
            },
            ProviderExecutionError::Stopped {
                message: "provider execution was cancelled while streaming".into(),
                facts: facts.clone(),
            },
            ProviderExecutionError::Stopped {
                message: "database could not persist provider output".into(),
                facts,
            },
            ProviderExecutionError::Interrupted {
                message: "provider execution was cancelled before a response arrived".into(),
            },
        ];
        let mut settled = Vec::new();
        for ending in endings {
            let interrupted = matches!(ending, ProviderExecutionError::Interrupted { .. });
            let job = local_child(&handle, agent, targeted);
            let input = record(&handle, job).input;
            let receipt = wait(settle_provider_error(
                handle.clone(),
                executor,
                input,
                job,
                RequestNumber::FIRST,
                ending,
            ))
            .expect("settle the execution");
            assert_eq!(
                receipt.result, None,
                "an unknown outcome publishes no result"
            );
            let job_record = record(&handle, job);
            assert!(
                job_record.unknown_at.is_some() && job_record.stop_confirmed_at.is_some(),
                "the outcome stays unknown and the local stop is confirmed"
            );
            assert_eq!(
                (
                    job_record.completed_at,
                    job_record.failed_at,
                    job_record.cancelled_at
                ),
                (None, None, None),
                "no remote outcome is claimed"
            );
            if interrupted {
                assert!(
                    wait(handle.job_availability(job, None, 10))
                        .expect("read observations")
                        .is_empty(),
                    "a local interruption observed nothing about the Provider"
                );
            }
            settled.push(job);
        }
        // In format 16, profiles are always editable (no lock checks).
        for job in settled {
            assert!(matches!(
                wait(handle.confirm_job_stopped(job)),
                Err(DatabaseError::Rejected(Violation::StopAlreadyConfirmed(_)))
            ));
        }
        worker.shutdown().expect("shutdown");

        let worker = DatabaseWorker::start(&directory).expect("reopen database");
        assert_eq!(
            worker.handle().abandoned_at_open(),
            [active],
            "settled Jobs left no claim behind"
        );
        worker.shutdown().expect("shutdown");
        assert_relationships_consistent(&directory);
    }

    #[test]
    fn a_settlement_that_was_not_committed_confirms_no_stop() {
        let (_root, directory) = data_directory("uncommitted-stop");
        let worker = DatabaseWorker::start_with_config(&directory, lenient()).expect("start");
        let handle = worker.handle();
        let Configured {
            targeted, agent, ..
        } = configured(&handle);
        let executor = executor(&handle, "llm worker", &[OPENAI]).expect("register Executor");
        let job = local_child(&handle, agent, targeted);
        let input = record(&handle, job).input;

        assert!(handle.activate_filesystem_protection(space_only(&directory)));
        match wait(settle_provider_error(
            handle.clone(),
            executor,
            input,
            job,
            RequestNumber::FIRST,
            ProviderExecutionError::Unknown {
                message: "connection reset".into(),
                facts: None,
                finished: false,
            },
        )) {
            Err(ExecutionRuntimeError::Database(error)) => assert!(
                error
                    .command_outcome()
                    .is_some_and(|outcome| outcome != crate::database::CommandOutcome::Unknown),
                "the settlement was known not to commit: {error}"
            ),
            other => panic!("a refused settlement must fail, got {other:?}"),
        }
        wait(handle.recover_storage()).expect("recover");
        let job_record = record(&handle, job);
        assert!(job_record.started_at.is_some());
        assert_eq!(
            (job_record.unknown_at, job_record.stop_confirmed_at),
            (None, None),
            "nothing was settled, so no stop is confirmed"
        );
        // In format 16, profiles are always editable (no lock checks).
        worker.shutdown().expect("shutdown");
    }

    #[test]
    fn an_abandoned_job_without_a_spawning_call_is_settled_by_the_next_start() {
        let (_root, directory) = data_directory("abandoned-job");
        let worker = DatabaseWorker::start(&directory).expect("start database");
        let handle = worker.handle();
        let job = local_job(
            &handle,
            new_job(&handle, "running", handle.genesis(), Vec::new()),
        );
        // Dropped without a finalization, as a dying process leaves it.
        assert!(record(&handle, job).started_at.is_some());
        worker.shutdown().expect("shutdown");

        let worker = DatabaseWorker::start(&directory).expect("restart");
        let handle = worker.handle();
        assert_eq!(handle.abandoned_at_open(), [job]);
        let job_record = record(&handle, job);
        // No admitted call can have started a process, so the stop is confirmed.
        assert!(job_record.unknown_at.is_some() && job_record.stop_confirmed_at.is_some());
        assert!(job_record.completed_at.is_none() && job_record.failed_at.is_none());
        assert!(job_record.subtree_settled);

        // A Job that ends normally leaves nothing to settle.
        let next = local_job(
            &handle,
            new_job(&handle, "next", handle.genesis(), Vec::new()),
        );
        wait(handle.finalize_job(next, JobFinalization::Completed(result("done"))))
            .expect("end the Job");
        worker.shutdown().expect("shutdown");
        let worker = DatabaseWorker::start(&directory).expect("restart");
        assert!(worker.handle().abandoned_at_open().is_empty());
        worker.shutdown().expect("shutdown");
    }

    /// A Provider endpoint that accepts connections and never answers, so a
    /// request to it stalls before any response head. The accepted
    /// connections stay open while the returned receiver holds them.
    pub(super) fn silent_provider(connections: usize) -> (String, flume::Receiver<TcpStream>) {
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind a local port");
        let port = listener.local_addr().expect("local address").port();
        let (sender, accepted) = flume::unbounded();
        std::thread::spawn(move || {
            for stream in listener.incoming().take(connections) {
                match stream {
                    Ok(stream) => {
                        if sender.send(stream).is_err() {
                            return;
                        }
                    }
                    Err(_) => return,
                }
            }
        });
        (format!("https://127.0.0.1:{port}/v1"), accepted)
    }

    struct SilentSetup {
        executor: ExecutorId,
        llm: ProfileId,
    }

    fn silent_setup(handle: &DatabaseHandle, endpoint: String) -> SilentSetup {
        let provider = connection(handle, "silent", Some(endpoint));
        let llm = llm_profile(
            handle,
            provider,
            llm_config("model-s", &[ModelCapability::Chat]),
        );
        let executor = executor(handle, "llm worker", &[OPENAI]).expect("register Executor");
        SilentSetup { executor, llm }
    }

    fn provider_plan(
        handle: &DatabaseHandle,
        setup: &SilentSetup,
        timeout: Duration,
    ) -> ProviderExecutionPlan {
        ProviderExecutionPlan {
            executor_id: setup.executor,
            input: JobInputDraft {
                objective: Some("answer".into()),
                ..JobInputDraft::default()
            },
            job: NewJob {
                configuration: llm_only(setup.llm),
                ..new_job(handle, "provider request", handle.genesis(), Vec::new())
            },
            credential_resolver: Arc::new(EnvironmentCredentialResolver),
            provider_config: ProviderExecutionConfig {
                timeout,
                cancellation: CancellationToken::default(),
                ..ProviderExecutionConfig::default()
            },
            tools: None,
        }
    }

    fn job_count(handle: &DatabaseHandle) -> usize {
        wait(handle.jobs(None, 1_000)).expect("read jobs").len()
    }

    #[test]
    fn a_timed_out_request_settles_unknown_with_its_local_stop_confirmed() {
        let (_root, directory) = data_directory("runtime-timeout");
        let worker = DatabaseWorker::start(&directory).expect("start database");
        let handle = worker.handle();
        let (endpoint, _accepted) = silent_provider(1);
        let setup = silent_setup(&handle, endpoint);
        let runtime = ExecutionRuntime::start(handle.clone()).expect("start runtime");
        let receipt = match wait(runtime.scheduler().schedule_provider(provider_plan(
            &handle,
            &setup,
            Duration::from_millis(300),
        ))) {
            Ok(receipt) => receipt,
            Err(ExecutionRuntimeError::ObservationsNotRecorded { receipt, source }) => {
                panic!("observations of {receipt:?} were not recorded: {source}")
            }
            Err(error) => panic!("the request must settle, got {error}"),
        };
        let job_record = record(&handle, receipt.job);
        assert!(job_record.unknown_at.is_some() && job_record.stop_confirmed_at.is_some());
        assert!(blocking(&handle, setup.llm).is_empty());
        wait(runtime.shutdown()).expect("shutdown runtime");
        worker.shutdown().expect("shutdown");

        let worker = DatabaseWorker::start(&directory).expect("restart");
        assert!(worker.handle().abandoned_at_open().is_empty());
        worker.shutdown().expect("shutdown");
    }

    #[test]
    fn shutdown_cancels_the_running_request_and_refuses_queued_plans_unpersisted() {
        let (_root, directory) = data_directory("runtime-shutdown");
        let worker = DatabaseWorker::start(&directory).expect("start database");
        let handle = worker.handle();
        let (endpoint, accepted) = silent_provider(1);
        let setup = silent_setup(&handle, endpoint);
        let runtime = ExecutionRuntime::start(handle.clone()).expect("start runtime");
        let scheduler = runtime.scheduler();

        // A request that would wait two minutes for a response head.
        let running = {
            let scheduler = scheduler.clone();
            let plan = provider_plan(&handle, &setup, Duration::from_secs(120));
            std::thread::spawn(move || wait(scheduler.schedule_provider(plan)))
        };
        let _connection = accepted
            .recv_timeout(crate::test_support::WAIT)
            .expect("the request reached the Provider");
        let jobs_running = job_count(&handle);

        // Plans queued behind it, of both kinds.
        let queued_provider = admit(scheduler.schedule_provider(provider_plan(
            &handle,
            &setup,
            Duration::from_secs(120),
        )));
        let queued_plain = admit(scheduler.schedule(ExecutionPlan {
            executor_id: setup.executor,
            input: JobInputDraft::default(),
            job: NewJob {
                configuration: llm_only(setup.llm),
                ..new_job(&handle, "plain plan", handle.genesis(), Vec::new())
            },
            outcome: JobOutcome::Completed,
            result: result("answered"),
        }));

        let started = Instant::now();
        wait(runtime.shutdown()).expect("shutdown runtime");
        assert!(
            started.elapsed() < Duration::from_secs(5),
            "shutdown waited for the request's timeout"
        );
        for result in [queued_provider(), queued_plain()] {
            match result {
                Err(ExecutionRuntimeError::ShuttingDown) => {}
                Ok(receipt) => assert!(record(&handle, receipt.job).any_terminal()),
                other => panic!("shutdown must refuse or settle an admission: {other:?}"),
            }
        }
        assert!(job_count(&handle) >= jobs_running);
        assert!(matches!(
            wait(scheduler.schedule_provider(provider_plan(
                &handle,
                &setup,
                Duration::from_secs(120)
            ))),
            Err(ExecutionRuntimeError::ShuttingDown)
        ));

        let receipt = running
            .join()
            .expect("the submitter finished")
            .expect("the cancelled request settles");
        assert_eq!(receipt.result, None);
        let job_record = record(&handle, receipt.job);
        assert!(
            job_record.unknown_at.is_some() && job_record.stop_confirmed_at.is_some(),
            "a request cancelled after it may have been sent is unknown, and stopped"
        );
        assert!(
            wait(handle.job_availability(receipt.job, None, 10))
                .expect("read observations")
                .is_empty(),
            "a local cancellation is no observation of the Provider"
        );
        assert!(blocking(&handle, setup.llm).is_empty());
        worker.shutdown().expect("shutdown");

        let worker = DatabaseWorker::start(&directory).expect("restart");
        assert!(worker.handle().abandoned_at_open().is_empty());
        worker.shutdown().expect("shutdown");
    }

    /// A started Job settled with `finalization`.
    fn settled_job(handle: &DatabaseHandle, name: &str, finalization: JobFinalization) -> JobId {
        let job = create_job(handle, new_job(handle, name, handle.genesis(), Vec::new()));
        wait(handle.claim_fixture(job, test_executor(handle))).expect("start");
        wait(handle.finalize_job(job, finalization)).expect("settle");
        job
    }

    #[test]
    fn execution_starts_only_once_every_dependency_is_satisfied() {
        let (_root, directory) = data_directory("start-gate");
        let worker = DatabaseWorker::start(&directory).expect("start database");
        let handle = worker.handle();
        let completed = settled_job(
            &handle,
            "completed",
            JobFinalization::Completed(result("done")),
        );
        let failed = settled_job(&handle, "failed", JobFinalization::Failed(result("failed")));
        let waiting = create_job(
            &handle,
            new_job(&handle, "waiting", handle.genesis(), Vec::new()),
        );
        let dependent = |dependencies: &[(JobId, DependencyCondition)]| {
            create_job(
                &handle,
                NewJob {
                    dependencies: dependencies.to_vec(),
                    ..new_job(&handle, "dependent", handle.genesis(), Vec::new())
                },
            )
        };

        for dependencies in [
            &[(completed, Success)][..],
            &[(failed, Settled)],
            &[(completed, Success), (failed, Settled)],
        ] {
            let job = dependent(dependencies);
            wait(handle.claim_fixture(job, test_executor(&handle)))
                .expect("a satisfied Job starts");
            let local = dependent(dependencies);
            wait(handle.claim_fixture(local, test_executor(&handle)))
                .expect("a satisfied Job starts locally");
            for job in [job, local] {
                assert!(record(&handle, job).started_at.is_some());
                wait(handle.finalize_job(job, JobFinalization::Completed(result("done"))))
                    .expect("and completes");
            }
        }

        let refused_creation = violation(wait(handle.create_job(NewJob {
            dependencies: vec![(failed, Success)],
            ..new_job(
                &handle,
                "impossible dependent",
                handle.genesis(),
                Vec::new(),
            )
        })));
        assert!(matches!(
            refused_creation,
            Violation::UnsatisfiedDependency { prerequisite, state: Impossible, .. } if prerequisite == failed
        ));

        let cases = [
            (&[(waiting, Success)][..], waiting, Waiting),
            (
                &[(completed, Success), (waiting, Settled)],
                waiting,
                Waiting,
            ),
        ];
        let mut refused = Vec::new();
        for (dependencies, prerequisite, state) in cases {
            let job = dependent(dependencies);
            let before = record(&handle, job);
            let expected = Violation::UnsatisfiedDependency {
                job,
                prerequisite,
                state,
            };
            assert_eq!(
                violation(wait(handle.claim_fixture(job, test_executor(&handle)))),
                expected
            );
            assert_eq!(
                violation(wait(handle.claim_fixture(job, test_executor(&handle)))),
                expected
            );
            assert_eq!(
                record(&handle, job),
                before,
                "a refused start changes nothing"
            );
            refused.push((job, state));
        }

        // A waiting Job starts once its dependency is satisfied; an
        // impossible one is closed only by an explicit failure.
        wait(handle.claim_fixture(waiting, test_executor(&handle)))
            .expect("start the prerequisite");
        wait(handle.finalize_job(waiting, JobFinalization::Completed(result("done"))))
            .expect("complete the prerequisite");
        for (job, state) in refused {
            if state == Waiting {
                wait(handle.claim_fixture(job, test_executor(&handle)))
                    .expect("the waiting Job now starts");
                assert!(record(&handle, job).started_at.is_some());
                wait(handle.finalize_job(job, JobFinalization::Completed(result("done"))))
                    .expect("and completes");
            } else {
                assert!(matches!(
                    violation(wait(handle.claim_fixture(job, test_executor(&handle)))),
                    Violation::UnsatisfiedDependency { .. }
                ));
                let unchanged = record(&handle, job);
                assert!(unchanged.started_at.is_none() && unchanged.failed_at.is_none());
                wait(
                    handle
                        .finalize_job(job, JobFinalization::Failed(result("prerequisite failed"))),
                )
                .expect("an explicit failure closes it");
            }
        }
        worker.shutdown().expect("shutdown");

        let worker = DatabaseWorker::start(&directory).expect("restart");
        assert!(
            worker.handle().abandoned_at_open().is_empty(),
            "no refused start left a claim"
        );
        worker.shutdown().expect("shutdown");
        assert_relationships_consistent(&directory);
    }
}

// ---- Token eligibility at ingest -------------------------------------------------

/// One Job's Token annotations agree on whether its request was sent; a
/// contradicting annotation is refused in the transaction that would write it.
mod token_eligibility_at_ingest {
    use super::*;
    use crate::token_usage::{RequestEligibility, TokenAnnotation, UsageFinality};

    fn fact(job: JobId, micros: i64, token: Option<TokenAnnotation>) -> NewMetrics {
        NewMetrics {
            job,
            request: RequestNumber::FIRST,
            observed_at: at(micros),
            measurements: Measurements {
                ttfb_ms: token.is_none().then_some(5),
                ..Measurements::default()
            },
            token,
        }
    }

    fn eligible() -> Option<TokenAnnotation> {
        Some(TokenAnnotation::eligible(UsageFinality::Cumulative))
    }

    fn excluded() -> Option<TokenAnnotation> {
        Some(TokenAnnotation::excluded())
    }

    fn contradiction(job: JobId, committed: RequestEligibility) -> Violation {
        let refused = match committed {
            RequestEligibility::Eligible => RequestEligibility::Excluded,
            RequestEligibility::Excluded => RequestEligibility::Eligible,
        };
        Violation::ContradictoryTokenEligibility {
            job,
            request: RequestNumber::FIRST,
            committed,
            refused,
        }
    }

    #[test]
    fn consistent_evidence_is_accepted_and_a_contradiction_is_refused_atomically() {
        let (_root, directory) = data_directory("ingest-eligibility");
        let worker = DatabaseWorker::start(&directory).expect("start database");
        let handle = worker.handle();
        let (_provider, llm) = llm_setup(&handle, "model-a");

        let mut excluded_job = None;
        for (first, committed) in [
            (eligible(), RequestEligibility::Eligible),
            (excluded(), RequestEligibility::Excluded),
        ] {
            let job = llm_job(&handle, llm);
            if committed == RequestEligibility::Excluded {
                excluded_job = Some(job);
            }
            // Generic Metrics carry no eligibility and never conflict.
            wait(handle.record_metrics(fact(job, 1, None))).expect("generic Metrics");
            wait(handle.record_metrics(fact(job, 2, first))).expect("the first annotation");
            wait(handle.record_metrics(fact(job, 3, first))).expect("a consistent repeat");
            wait(handle.record_metrics(fact(job, 4, None))).expect("generic Metrics after");

            let contradicting = match committed {
                RequestEligibility::Eligible => excluded(),
                RequestEligibility::Excluded => eligible(),
            };
            assert_eq!(
                violation(wait(handle.record_metrics(fact(job, 5, contradicting)))),
                contradiction(job, committed)
            );
        }

        worker.shutdown().expect("shutdown");

        // The check holds for evidence committed by an earlier opening.
        let worker = DatabaseWorker::start(&directory).expect("reopen");
        let handle = worker.handle();
        let job = excluded_job.expect("the excluded Job");
        assert_eq!(
            violation(wait(handle.record_metrics(fact(job, 6, eligible())))),
            contradiction(job, RequestEligibility::Excluded)
        );
        worker.shutdown().expect("shutdown");
    }

    #[test]
    fn concurrent_conflicting_submissions_commit_one_eligibility() {
        let (_root, directory) = data_directory("ingest-concurrent");
        let worker = DatabaseWorker::start(&directory).expect("start database");
        let handle = worker.handle();
        let (_provider, llm) = llm_setup(&handle, "model-a");

        // Both queued before either runs: admission order decides.
        let job = llm_job(&handle, llm);
        let first = admit(handle.record_metrics(fact(job, 1, excluded())));
        let second = admit(handle.record_metrics(fact(job, 2, eligible())));
        first().expect("the first in admission order is accepted");
        assert_eq!(
            violation(second()),
            contradiction(job, RequestEligibility::Excluded)
        );

        // Many callers on many threads, through the one writer.
        let job = llm_job(&handle, llm);
        let submitters: Vec<_> = (0..16)
            .map(|index| {
                let handle = handle.clone();
                std::thread::spawn(move || {
                    let token = if index % 2 == 0 {
                        eligible()
                    } else {
                        excluded()
                    };
                    (token, wait(handle.record_metrics(fact(job, index, token))))
                })
            })
            .collect();
        let results: Vec<_> = submitters
            .into_iter()
            .map(|submitter| submitter.join().expect("submitter"))
            .collect();
        let accepted: Vec<_> = results
            .iter()
            .filter(|(_, result)| result.is_ok())
            .map(|(token, _)| *token)
            .collect();
        assert!(!accepted.is_empty());
        assert!(
            accepted.iter().all(|token| *token == accepted[0]),
            "every accepted annotation states one eligibility"
        );
        for (token, result) in &results {
            if *token != accepted[0] {
                assert!(matches!(
                    result,
                    Err(DatabaseError::Rejected(
                        Violation::ContradictoryTokenEligibility { .. }
                    ))
                ));
            }
        }
        worker.shutdown().expect("shutdown");
    }
}

// ---- aggregation liveness ----------------------------------------------------------

// A loop that exits is never reported running, a known rollback is retried
// from committed progress, and an unknown commit is resolved, not replayed.

// ---- settlement, dependencies and retries --------------------------------------

mod settlement {
    use std::collections::BTreeSet;
    use std::num::NonZeroU32;

    use super::*;
    use crate::persistence::{
        AuthorizationBasis, CapabilitySet, ExecutionOperation, ExecutionPoolConfig,
        NewExecutionPool, NewToolEvidence, ToolAdmission, ToolCallRef, ToolDecision, ToolEffect,
        ToolPayload,
    };
    use DependencyCondition::{Settled, Success};

    /// A Pool that runs `limit` LLM requests over the OpenAI protocol, and the
    /// Model config whose Jobs it can run.
    fn setup_pool(handle: &DatabaseHandle, limit: u32) -> ProfileId {
        let (_provider, llm) = llm_setup(handle, "settlement-model");
        wait(handle.create_execution_pool(NewExecutionPool {
            name: "test-pool".into(),
            config: ExecutionPoolConfig {
                concurrency_limit: NonZeroU32::new(limit).unwrap(),
                capabilities: CapabilitySet {
                    operations: BTreeSet::from([ExecutionOperation::LlmRequest]),
                    llm_protocols: BTreeSet::from([OPENAI]),
                },
            },
            enabled: true,
        }))
        .expect("create pool");
        llm
    }

    fn job(handle: &DatabaseHandle, llm: ProfileId, name: &str) -> JobId {
        create_job(
            handle,
            NewJob {
                configuration: llm_only(llm),
                ..new_job(handle, name, JobId::new(0), Vec::new())
            },
        )
    }

    fn dependent(
        handle: &DatabaseHandle,
        llm: ProfileId,
        name: &str,
        prerequisite: JobId,
        condition: DependencyCondition,
    ) -> JobId {
        create_job(
            handle,
            NewJob {
                configuration: llm_only(llm),
                dependencies: vec![(prerequisite, condition)],
                ..new_job(handle, name, JobId::new(0), Vec::new())
            },
        )
    }

    fn record(handle: &DatabaseHandle, job: JobId) -> JobRecord {
        wait(handle.job(job))
            .expect("read job")
            .expect("job exists")
    }

    /// An admitted call that spawns a process.
    fn spawning(call: ToolCallRef) -> NewToolEvidence {
        NewToolEvidence {
            call,
            tool_call_id: "run".into(),
            name: "workspace_run_command".into(),
            decision: ToolDecision::Admitted(ToolAdmission {
                handler_version: "test-1".into(),
                effect: ToolEffect::NonIdempotent,
                parameters: "{}".into(),
                arguments: ToolPayload::new("{}"),
                authorization: AuthorizationBasis::RegistryGrant,
            }),
            spawns: true,
        }
    }

    #[test]
    fn format_16_settlement_success_dependency_satisfied_on_completion() {
        let (_root, directory) = data_directory("f16-settle-success");
        let worker = DatabaseWorker::start(&directory).expect("start database");
        let handle = worker.handle();
        let llm = setup_pool(&handle, 2);

        let job_a = job(&handle, llm, "job-a");
        let job_b = dependent(&handle, llm, "job-b", job_a, Success);

        // Job A is ready; Job B is waiting on Job A
        let grants = wait(handle.claim_batch()).expect("claim batch");
        assert_eq!(grants, vec![job_a]);

        // Complete Job A; SETTLE marks Job A SubtreeSettled and places Job B into ready
        wait(handle.finalize_job(job_a, JobFinalization::Completed(result("A done"))))
            .expect("finalize A");

        let rec_a = record(&handle, job_a);
        assert!(rec_a.completed_at.is_some());

        // Now Job B is ready and can be claimed
        let grants_b = wait(handle.claim_batch()).expect("claim batch");
        assert_eq!(grants_b, vec![job_b]);

        wait(handle.finalize_job(job_b, JobFinalization::Completed(result("B done"))))
            .expect("finalize B");

        let rec_b = record(&handle, job_b);
        assert!(rec_b.completed_at.is_some());

        worker.shutdown().expect("shutdown");
        assert_relationships_consistent(&directory);
    }

    #[test]
    fn format_16_settlement_success_dependency_impossible_cancels_waiter() {
        let (_root, directory) = data_directory("f16-settle-failed-waiter");
        let worker = DatabaseWorker::start(&directory).expect("start database");
        let handle = worker.handle();
        let llm = setup_pool(&handle, 2);

        let job_a = job(&handle, llm, "job-a");
        let job_b = dependent(&handle, llm, "job-b", job_a, Success);

        let grants = wait(handle.claim_batch()).expect("claim batch");
        assert_eq!(grants, vec![job_a]);

        // Fail Job A; SETTLE detects that Success dependency on Job A is impossible,
        // and automatically cancels Job B
        wait(handle.finalize_job(job_a, JobFinalization::Failed(result("A failed"))))
            .expect("finalize A");

        let rec_a = record(&handle, job_a);
        assert!(rec_a.failed_at.is_some());

        let rec_b = record(&handle, job_b);
        assert!(
            rec_b.cancelled_at.is_some(),
            "waiter Job B was cancelled by SETTLE"
        );

        // Job B was cancelled and settled, so claim batch has no work
        let grants_empty = wait(handle.claim_batch()).expect("claim batch");
        assert!(grants_empty.is_empty());

        worker.shutdown().expect("shutdown");
        assert_relationships_consistent(&directory);
    }

    #[test]
    fn format_16_settlement_settled_dependency_satisfied_on_failure() {
        let (_root, directory) = data_directory("f16-settle-settled-dep");
        let worker = DatabaseWorker::start(&directory).expect("start database");
        let handle = worker.handle();
        let llm = setup_pool(&handle, 2);

        let job_a = job(&handle, llm, "job-a");
        let job_b = dependent(&handle, llm, "job-b", job_a, Settled);

        let grants = wait(handle.claim_batch()).expect("claim batch");
        assert_eq!(grants, vec![job_a]);

        // Fail Job A; since Job B required Settled (not Success), Job B is satisfied!
        wait(handle.finalize_job(job_a, JobFinalization::Failed(result("A failed"))))
            .expect("finalize A");

        let grants_b = wait(handle.claim_batch()).expect("claim batch");
        assert_eq!(grants_b, vec![job_b]);

        wait(handle.finalize_job(job_b, JobFinalization::Completed(result("B done"))))
            .expect("finalize B");

        worker.shutdown().expect("shutdown");
        assert_relationships_consistent(&directory);
    }

    #[test]
    fn format_16_settlement_parent_closes_and_settles_when_children_settle() {
        let (_root, directory) = data_directory("f16-settle-parent-child");
        let worker = DatabaseWorker::start(&directory).expect("start database");
        let handle = worker.handle();
        let llm = setup_pool(&handle, 2);

        let parent = job(&handle, llm, "parent");
        let grants_p = wait(handle.claim_batch()).expect("claim parent");
        assert_eq!(grants_p, vec![parent]);

        // Create child under parent
        let child = create_job(
            &handle,
            NewJob {
                configuration: llm_only(llm),
                ..new_job(&handle, "child", parent, Vec::new())
            },
        );
        let grants_c = wait(handle.claim_batch()).expect("claim child");
        assert_eq!(grants_c, vec![child]);

        // Finalize parent while child is still open -> coerced to Unknown under I6
        wait(handle.finalize_job(parent, JobFinalization::Completed(result("P done"))))
            .expect("finalize parent");

        let rec_p = record(&handle, parent);
        assert!(
            rec_p.unknown_at.is_some(),
            "parent outcome coerced to Unknown by I6"
        );
        assert!(
            !rec_p.subtree_settled,
            "an open child keeps the subtree unsettled"
        );

        // Finalize child -> child settles and decrements parent open_children
        wait(handle.finalize_job(child, JobFinalization::Completed(result("C done"))))
            .expect("finalize child");

        let rec_c = record(&handle, child);
        assert!(rec_c.completed_at.is_some());
        assert!(rec_c.subtree_settled);
        assert!(
            record(&handle, parent).subtree_settled,
            "the last child settles the parent"
        );

        worker.shutdown().expect("shutdown");
        assert_relationships_consistent(&directory);
    }

    #[test]
    fn format_16_claim_batch_fills_the_lowest_pool_first_and_counts_held_slots() {
        let (_root, directory) = data_directory("f16-claim-batch-pools");
        let worker = DatabaseWorker::start(&directory).expect("start database");
        let handle = worker.handle();
        let llm = setup_pool(&handle, 1);
        let second_pool = wait(handle.create_execution_pool(NewExecutionPool {
            name: "second-pool".into(),
            config: ExecutionPoolConfig {
                concurrency_limit: NonZeroU32::MIN,
                capabilities: CapabilitySet {
                    operations: BTreeSet::from([ExecutionOperation::LlmRequest]),
                    llm_protocols: BTreeSet::from([OPENAI]),
                },
            },
            enabled: true,
        }))
        .expect("create the second pool");
        let first_pool = wait(handle.execution_pools(None, 10))
            .expect("pools")
            .into_iter()
            .map(|pool| pool.id)
            .min()
            .expect("a pool");
        assert!(first_pool < second_pool);

        // The smallest Job id goes first, into the Pool with the lowest id
        // among those that offer the same capabilities.
        let j1 = job(&handle, llm, "j1");
        let j2 = job(&handle, llm, "j2");
        let j3 = job(&handle, llm, "j3");
        assert_eq!(wait(handle.claim_batch()).expect("claim"), vec![j1, j2]);
        let pool_of = |job| record(&handle, job).claim.expect("claimed").pool_id;
        assert_eq!(pool_of(j1), first_pool.into());
        assert_eq!(pool_of(j2), second_pool.into());

        // A Job whose outcome is unknown while a process it spawned may still
        // run keeps its slot, so nothing else is granted into that Pool.
        let run = ToolCallRef {
            job: j1,
            request: RequestNumber::FIRST,
            index: 0,
        };
        wait(handle.record_metrics(NewMetrics {
            job: j1,
            request: RequestNumber::FIRST,
            observed_at: Timestamp::now(),
            measurements: Measurements::default(),
            token: Some(TokenAnnotation::eligible(UsageFinality::Cumulative)),
        }))
        .expect("record the delivery");
        wait(handle.record_tool_decision(spawning(run))).expect("admit");
        wait(handle.finalize_job(j1, JobFinalization::Unknown)).expect("unknown");
        assert!(record(&handle, j1).stop_confirmed_at.is_none());
        wait(handle.finalize_job(j2, JobFinalization::Completed(result("j2 done"))))
            .expect("finalize j2");
        assert_eq!(wait(handle.claim_batch()).expect("claim"), vec![j3]);
        assert_eq!(pool_of(j3), second_pool.into());
        wait(handle.finalize_job(j3, JobFinalization::Completed(result("j3 done"))))
            .expect("finalize j3");
        let j4 = job(&handle, llm, "j4");
        assert_eq!(wait(handle.claim_batch()).expect("claim"), vec![j4]);
        assert_eq!(pool_of(j4), second_pool.into(), "the held slot is not free");
        wait(handle.confirm_job_stopped(j1)).expect("confirm");
        wait(handle.finalize_job(j4, JobFinalization::Completed(result("j4 done"))))
            .expect("finalize j4");
        let j5 = job(&handle, llm, "j5");
        assert_eq!(wait(handle.claim_batch()).expect("claim"), vec![j5]);
        assert_eq!(
            pool_of(j5),
            first_pool.into(),
            "a confirmed stop frees the slot"
        );

        worker.shutdown().expect("shutdown");
        assert_relationships_consistent(&directory);
    }

    #[test]
    fn format_16_claim_batch_concurrency_limit() {
        let (_root, directory) = data_directory("f16-claim-batch-limit");
        let worker = DatabaseWorker::start(&directory).expect("start database");
        let handle = worker.handle();
        let llm = setup_pool(&handle, 2);

        let j1 = job(&handle, llm, "j1");
        let j2 = job(&handle, llm, "j2");
        let j3 = job(&handle, llm, "j3");

        // Pool has concurrency limit 2, so only 2 jobs should be claimed
        let grants = wait(handle.claim_batch()).expect("claim batch");
        assert_eq!(grants.len(), 2);
        assert!(grants.contains(&j1));
        assert!(grants.contains(&j2));
        assert!(!grants.contains(&j3));

        // Finalize j1 -> slot is freed
        wait(handle.finalize_job(j1, JobFinalization::Completed(result("j1 done"))))
            .expect("finalize j1");

        // Now j3 can be claimed
        let grants2 = wait(handle.claim_batch()).expect("claim batch 2");
        assert_eq!(grants2, vec![j3]);

        wait(handle.finalize_job(j2, JobFinalization::Completed(result("j2 done"))))
            .expect("finalize j2");
        wait(handle.finalize_job(j3, JobFinalization::Completed(result("j3 done"))))
            .expect("finalize j3");

        worker.shutdown().expect("shutdown");
        assert_relationships_consistent(&directory);
    }
}

// ---- Request-level evidence and the tool loop ----------------------------------

/// Format 11: every Provider request of a Job has its own evidence, a format
/// 10 file is upgraded in place, and the tool loop runs OCG's own tools
/// between requests.
mod requests_and_tool_loop {
    use std::collections::BTreeMap;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use serde_json::Value;

    use super::*;
    use crate::openai_compatible::{ChatMessage, MessageContentWire, ToolCallRefusal};
    use crate::orchestration::execution_runtime::{
        DatabaseOutputSink, ExecutionReceipt, ProviderExecutionPlan, ProviderRun, ToolExecution,
        ToolLoopLimits, ToolLoopStop, run_provider_job,
    };
    use crate::orchestration::tools::{ToolFailure, ToolFuture, ToolHandler, ToolRegistry};
    use crate::persistence::ToolEffect;
    use crate::provider_execution::ProviderExecutionError;

    fn number(value: u32) -> RequestNumber {
        RequestNumber::new(value).expect("a request number")
    }

    /// What one answered request leaves: its delivery, its final usage and
    /// its availability observation.
    fn answered_request(
        handle: &DatabaseHandle,
        job: JobId,
        value: u32,
        micros: i64,
        finish: ChatFinishReason,
        (input, output): (u32, u32),
    ) {
        let request = number(value);
        for (offset, usage, finality) in [
            (0, NormalizedUsage::default(), UsageFinality::Cumulative),
            (
                1,
                NormalizedUsage {
                    input_tokens: Some(input),
                    output_tokens: Some(output),
                    ..NormalizedUsage::default()
                },
                UsageFinality::Final,
            ),
        ] {
            wait(handle.record_metrics(NewMetrics {
                job,
                request,
                observed_at: at(micros + offset),
                measurements: Measurements {
                    usage,
                    ..Measurements::default()
                },
                token: Some(TokenAnnotation::eligible(finality)),
            }))
            .expect("record Metrics");
        }
        wait(handle.record_availability_stat(NewAvailabilityStat {
            job,
            request,
            observed_at: at(micros),
            observation: AvailabilityObservation {
                status_code: Some(200),
                finish_reason: Some(finish),
                raw_finish_reason: Some(finish.as_openai_str().to_owned()),
                failure: None,
            },
        }))
        .expect("record an AvailabilityStat");
    }

    #[test]
    fn the_requests_of_one_job_are_reduced_and_counted_apart() {
        let (_root, directory) = data_directory("per-request-evidence");
        let worker = DatabaseWorker::start(&directory).expect("start database");
        let handle = worker.handle();
        let (_provider, llm) = llm_setup(&handle, "model-a");
        let job = llm_job(&handle, llm);
        let micros = 7 * 60_000_000;
        answered_request(
            &handle,
            job,
            1,
            micros,
            ChatFinishReason::ToolCalls,
            (10, 5),
        );
        answered_request(
            &handle,
            job,
            2,
            micros + 10,
            ChatFinishReason::Stop,
            (20, 7),
        );
        // A request that was never sent may follow one that was.
        wait(handle.record_metrics(NewMetrics {
            job,
            request: number(3),
            observed_at: at(micros + 20),
            measurements: Measurements::default(),
            token: Some(TokenAnnotation::excluded()),
        }))
        .expect("another request of the Job is excluded on its own");
        assert_eq!(
            violation(wait(handle.record_metrics(NewMetrics {
                job,
                request: number(2),
                observed_at: at(micros + 30),
                measurements: Measurements::default(),
                token: Some(TokenAnnotation::excluded()),
            }))),
            Violation::ContradictoryTokenEligibility {
                job,
                request: number(2),
                committed: RequestEligibility::Eligible,
                refused: RequestEligibility::Excluded,
            },
            "one request still cannot contradict itself"
        );

        let stats = stats(&handle, job);
        assert!(
            matches!(
                AvailabilityRequestSample::from_records(&stats),
                Err(MixedRequests { .. })
            ),
            "two requests are never reduced as one"
        );
        let of = |value| {
            stats
                .iter()
                .filter(move |stat| stat.request == number(value))
        };
        assert_eq!(
            AvailabilityRequestSample::from_records(of(1))
                .expect("one request")
                .map(|sample| sample.classification),
            Some(AvailabilityClassification::Unconfirmed),
            "a response asking for tools witnesses no normal termination"
        );
        assert_eq!(
            AvailabilityRequestSample::from_records(of(2))
                .expect("one request")
                .map(|sample| sample.classification),
            Some(AvailabilityClassification::Available),
            "the final answer's evidence is not made contradictory by the request before it"
        );
        let metrics = job_metrics(&handle, job);
        assert!(matches!(
            TokenRequestSample::from_metrics(&metrics),
            Err(crate::token_usage::TokenEvidenceError::MixedRequests(_))
        ));
        let samples = TokenRequestSample::per_request(&metrics).expect("per request");
        assert_eq!(
            samples
                .iter()
                .map(|sample| (sample.request, sample.input, sample.output))
                .collect::<Vec<_>>(),
            [
                (number(1), TokenCount::Final(10), TokenCount::Final(5)),
                (number(2), TokenCount::Final(20), TokenCount::Final(7)),
            ],
            "each request keeps its own finals; the excluded one has no sample"
        );
        assert_eq!(
            TokenRequestSample::job_total(&samples),
            Some(42),
            "a sum, never a maximum"
        );

        worker.shutdown().expect("shutdown");
    }

    // ---- The tool loop --------------------------------------------------------

    /// How a test tool behaves. Every one is free of side effects outside
    /// the test.
    enum Behavior {
        /// Adds integers `a` and `b`.
        Add,
        Fail,
        /// Returns a string of this many bytes.
        Big(usize),
        /// Never finishes.
        Hang,
        /// Cancels the execution, then never finishes.
        CancelThenHang(CancellationToken),
        /// Withdraws the grant of another tool, then answers.
        Revoke(ToolRegistry, &'static str),
        /// Fails without evidence that its effect did not happen.
        Uncertain,
        /// Panics while it runs.
        Crash,
        /// Does this, then answers what it returns.
        Act(Box<dyn Fn() -> Value + Send + Sync>),
    }

    struct TestTool {
        behavior: Behavior,
        calls: Arc<AtomicUsize>,
        effect: ToolEffect,
    }

    impl ToolHandler for TestTool {
        fn version(&self) -> &str {
            "test-1"
        }

        fn effect(&self) -> ToolEffect {
            self.effect
        }

        fn call(&self, arguments: Value) -> ToolFuture<'_> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            Box::pin(async move {
                match &self.behavior {
                    Behavior::Add => arguments["a"]
                        .as_i64()
                        .zip(arguments["b"].as_i64())
                        .and_then(|(a, b)| a.checked_add(b))
                        .map(|sum| json!({"sum": sum}))
                        .ok_or_else(|| ToolFailure::not_applied("no sum")),
                    Behavior::Fail => Err(ToolFailure::not_applied("the tool failed")),
                    Behavior::Big(bytes) => Ok(Value::String("x".repeat(*bytes))),
                    Behavior::Hang => std::future::pending().await,
                    Behavior::CancelThenHang(token) => {
                        token.cancel();
                        std::future::pending().await
                    }
                    Behavior::Revoke(registry, name) => {
                        registry.revoke(name).expect("revoke");
                        Ok(json!({"revoked": name}))
                    }
                    Behavior::Uncertain => Err(ToolFailure::uncertain("the connection dropped")),
                    Behavior::Crash => panic!("the process died while the handler ran"),
                    Behavior::Act(act) => Ok(act()),
                }
            })
        }
    }

    fn add_schema() -> Value {
        json!({
            "type": "object",
            "properties": {"a": {"type": "integer"}, "b": {"type": "integer"}},
            "required": ["a", "b"],
            "additionalProperties": false
        })
    }

    /// Registers and authorizes the tools as read-only; their handlers share
    /// one count of the calls they ran.
    fn registered(tools: Vec<(&str, Behavior)>) -> (ToolRegistry, Arc<AtomicUsize>) {
        registered_as(ToolEffect::ReadOnly, tools)
    }

    /// Like [`registered`], with every handler declaring `effect`.
    fn registered_as(
        effect: ToolEffect,
        tools: Vec<(&str, Behavior)>,
    ) -> (ToolRegistry, Arc<AtomicUsize>) {
        let registry = ToolRegistry::default();
        let calls = Arc::new(AtomicUsize::new(0));
        for (name, behavior) in tools {
            let handler = Arc::new(TestTool {
                behavior,
                calls: calls.clone(),
                effect,
            });
            registry
                .register(name, Some("a test tool"), add_schema(), handler)
                .expect("register");
            registry.authorize(name).expect("authorize");
        }
        (registry, calls)
    }

    fn execution(registry: &ToolRegistry, names: &[&str], limits: ToolLoopLimits) -> ToolExecution {
        ToolExecution {
            offer: registry.offer(names).expect("offer"),
            limits,
        }
    }

    fn chunk(text: String) -> Step {
        Step::Chunk(Box::leak(text.into_boxed_str()))
    }

    fn usage_chunk((input, output): (u32, u32)) -> Step {
        chunk(format!(
            "data: {{\"choices\":[],\"usage\":{{\"prompt_tokens\":{input},\"completion_tokens\":{output}}}}}\n\n"
        ))
    }

    /// One tool-call delta of a response.
    fn call(index: u32, id: &str, name: &str, arguments: &str) -> String {
        format!(
            r#"{{"index":{index},"id":"{id}","type":"function","function":{{"name":"{name}","arguments":{}}}}}"#,
            serde_json::to_string(arguments).expect("JSON string")
        )
    }

    /// A response that requests `calls` and finishes with `tool_calls`.
    fn calling(calls: &[String], usage: (u32, u32)) -> Vec<Step> {
        vec![
            chunk(format!(
                "data: {{\"choices\":[{{\"delta\":{{\"tool_calls\":[{}]}}}}]}}\n\n",
                calls.join(",")
            )),
            chunk(
                "data: {\"choices\":[{\"delta\":{},\"finish_reason\":\"tool_calls\"}]}\n\n".into(),
            ),
            usage_chunk(usage),
            Step::Chunk(DONE_CHUNK),
        ]
    }

    /// A final answer.
    fn answering(text: &str, usage: (u32, u32)) -> Vec<Step> {
        vec![
            chunk(format!(
                "data: {{\"choices\":[{{\"delta\":{{\"content\":{}}}}}]}}\n\n",
                serde_json::to_string(text).expect("JSON string")
            )),
            Step::Chunk(STOP_CHUNK),
            usage_chunk(usage),
            Step::Chunk(DONE_CHUNK),
        ]
    }

    fn add(index: u32, id: &str, a: i64, b: i64) -> String {
        call(index, id, "add", &format!(r#"{{"a":{a},"b":{b}}}"#))
    }

    /// A Job that this process started itself, so its claim is observable.
    fn local_run(handle: &DatabaseHandle, llm: ProfileId) -> TokenRun {
        let executor = executor(handle, "llm worker", &[OPENAI]).expect("register");
        let job = llm_job(handle, llm);
        wait(handle.claim_fixture(job, test_executor(handle))).expect("start the Job locally");
        let input = wait(handle.job(job)).expect("read").expect("job").input;
        TokenRun {
            executor,
            job,
            input,
        }
    }

    struct Looped {
        settled: Result<ExecutionReceipt, ExecutionRuntimeError>,
        /// Every conversation the loop sent, in order.
        sent: Vec<ChatConversation>,
    }

    /// Runs the real tool loop, dispatcher and settlement on `run`. Each
    /// request is checked as the transport checks it and answered by the
    /// next scripted response, read by the real reader; `after` runs once a
    /// response has been read, before anything is dispatched.
    fn run_loop(
        handle: &DatabaseHandle,
        run: TokenRun,
        tools: Option<ToolExecution>,
        responses: Vec<Vec<Step>>,
        cancellation: CancellationToken,
        mut after: impl FnMut(usize) + 'static,
    ) -> Looped {
        let handle = handle.clone();
        on_ntex(async move {
            let config = ProviderExecutionConfig {
                cancellation: cancellation.clone(),
                ..ProviderExecutionConfig::default()
            };
            let conversation = ChatConversation {
                messages: vec![ChatMessage::user("add the numbers")],
                tools: tools
                    .as_ref()
                    .map(|tools| tools.offer.wires().expect("offer"))
                    .unwrap_or_default(),
                ..ChatConversation::default()
            };
            let mut responses = responses.into_iter();
            let mut sent = Vec::new();
            let settled = run_provider_job(
                ProviderRun {
                    database: handle,
                    executor: run.executor,
                    input: run.input,
                    job: run.job,
                },
                conversation,
                tools.as_ref(),
                &cancellation,
                async |conversation: &ChatConversation, sink: &mut DatabaseOutputSink| {
                    sent.push(conversation.clone());
                    conversation
                        .validate()
                        .map_err(|error| ProviderExecutionError::Build {
                            message: error.to_string(),
                        })?;
                    let steps = responses.next().expect("a response for every request");
                    announce(Some(&mut *sink), ProviderFact::Delivered).await;
                    let response = read_response(
                        payload(steps, config.cancellation.clone()),
                        200,
                        Instant::now(),
                        None,
                        &config,
                        Some(&mut *sink),
                        conversation,
                    )
                    .await;
                    after(sent.len());
                    response
                },
            )
            .await;
            Looped { settled, sent }
        })
    }

    fn job_record(handle: &DatabaseHandle, job: JobId) -> JobRecord {
        wait(handle.job(job))
            .expect("read job")
            .expect("job exists")
    }

    /// The Metrics and AvailabilityStats of each request of `job`.
    fn by_request(
        handle: &DatabaseHandle,
        job: JobId,
    ) -> BTreeMap<u32, (Vec<MetricsRecord>, Vec<AvailabilityStatRecord>)> {
        let mut requests: BTreeMap<u32, (Vec<_>, Vec<_>)> = BTreeMap::new();
        for record in job_metrics(handle, job) {
            requests
                .entry(record.request.get())
                .or_default()
                .0
                .push(record);
        }
        for stat in stats(handle, job) {
            requests.entry(stat.request.get()).or_default().1.push(stat);
        }
        requests
    }

    fn stopped(looped: Looped) -> (ExecutionReceipt, ToolLoopStop) {
        match looped.settled {
            Err(ExecutionRuntimeError::ToolLoopStopped {
                receipt,
                stop,
                observations: None,
                evidence: None,
            }) => (*receipt, stop),
            other => panic!("expected the tool loop to stop, got {other:?}"),
        }
    }

    #[test]
    fn an_authorized_tool_runs_and_its_result_returns_for_the_final_answer() {
        let (_root, directory) = data_directory("tool-loop");
        let worker = DatabaseWorker::start(&directory).expect("start database");
        let handle = worker.handle();
        let (_provider, llm) = llm_setup(&handle, "model-a");
        let run = local_run(&handle, llm);
        let (registry, calls) = registered(vec![("add", Behavior::Add)]);
        let looped = run_loop(
            &handle,
            run,
            Some(execution(&registry, &["add"], ToolLoopLimits::default())),
            vec![
                calling(&[add(0, "call_1", 2, 3)], (11, 4)),
                answering("The sum is 5.", (30, 6)),
            ],
            CancellationToken::default(),
            |_| {},
        );
        let receipt = looped.settled.expect("the loop settles");
        assert_eq!(receipt.job, run.job);
        assert_eq!(calls.load(Ordering::SeqCst), 1, "the handler ran once");

        assert_eq!(looped.sent.len(), 2);
        assert_eq!(
            serde_json::to_value(&looped.sent[0]).expect("JSON"),
            json!({
                "messages": [{"role": "user", "content": "add the numbers"}],
                "tools": [{"type": "function", "function": {
                    "name": "add", "description": "a test tool", "parameters": add_schema()
                }}]
            }),
            "only the OCG tool is offered, with OCG's own schema"
        );
        assert_eq!(
            serde_json::to_value(&looped.sent[1].messages).expect("JSON"),
            json!([
                {"role": "user", "content": "add the numbers"},
                {"role": "assistant", "tool_calls": [{"id": "call_1", "type": "function",
                    "function": {"name": "add", "arguments": "{\"a\":2,\"b\":3}"}}]},
                {"role": "tool", "content": "{\"sum\":5}", "tool_call_id": "call_1"}
            ]),
            "the call and its result go back in order, with the call's id"
        );

        let job = job_record(&handle, run.job);
        assert!(job.completed_at.is_some() && job.stop_confirmed_at.is_none());
        let outputs = wait(handle.job_outputs(run.job, None, 100)).expect("outputs");
        let result = outputs.last().expect("a result");
        assert_eq!(result.kind, OutputKind::Result);
        assert_eq!(result.summary.as_deref(), Some("The sum is 5."));
        assert!(blocking(&handle, llm).is_empty(), "the Profile is released");

        // Each request keeps its own facts.
        let requests = by_request(&handle, run.job);
        assert_eq!(requests.keys().copied().collect::<Vec<_>>(), [1, 2]);
        for (request, finish) in [
            (1, ChatFinishReason::ToolCalls),
            (2, ChatFinishReason::Stop),
        ] {
            let (metrics, stats) = &requests[&request];
            assert_eq!(
                stats
                    .iter()
                    .map(|stat| (stat.observation.status_code, stat.observation.finish_reason))
                    .collect::<Vec<_>>(),
                [(Some(200), Some(finish))],
                "request {request}"
            );
            assert!(stats.iter().all(|stat| stat.observation.failure.is_none()));
            assert_eq!(
                metrics
                    .iter()
                    .filter(|record| record.token.is_none()
                        && record.measurements.ttfb_ms.is_some())
                    .count(),
                1,
                "request {request} has its own timing"
            );
        }
        let samples =
            TokenRequestSample::per_request(&job_metrics(&handle, run.job)).expect("per request");
        assert_eq!(
            samples
                .iter()
                .map(|sample| sample.total)
                .collect::<Vec<_>>(),
            [Some(15), Some(36)]
        );
        assert_eq!(TokenRequestSample::job_total(&samples), Some(51));
        worker.shutdown().expect("shutdown");
        let worker = DatabaseWorker::start(&directory).expect("restart");
        assert!(
            worker.handle().abandoned_at_open().is_empty(),
            "the local claim was released"
        );
        worker.shutdown().expect("shutdown");
    }

    #[test]
    fn sequential_calls_over_several_requests_keep_their_ids_and_order() {
        let (_root, directory) = data_directory("tool-loop-rounds");
        let worker = DatabaseWorker::start(&directory).expect("start database");
        let handle = worker.handle();
        let (_provider, llm) = llm_setup(&handle, "model-a");
        let run = local_run(&handle, llm);
        let (registry, calls) = registered(vec![("add", Behavior::Add)]);
        let looped = run_loop(
            &handle,
            run,
            Some(execution(&registry, &["add"], ToolLoopLimits::default())),
            vec![
                calling(&[add(0, "a", 1, 2), add(1, "b", 3, 4)], (1, 1)),
                calling(&[add(0, "c", 5, 6)], (2, 2)),
                answering("3, 7 and 11.", (3, 3)),
            ],
            CancellationToken::default(),
            |_| {},
        );
        looped.settled.expect("the loop settles");
        assert_eq!(calls.load(Ordering::SeqCst), 3);
        let roles_and_ids = looped.sent[2]
            .messages
            .iter()
            .map(|message| {
                let ids = message
                    .tool_calls
                    .iter()
                    .flatten()
                    .map(|call| call.id.clone())
                    .chain(message.tool_call_id.clone())
                    .collect::<Vec<_>>();
                (format!("{:?}", message.role), ids)
            })
            .collect::<Vec<_>>();
        assert_eq!(
            roles_and_ids,
            [
                ("User".to_owned(), vec![]),
                ("Assistant".to_owned(), vec!["a".to_owned(), "b".to_owned()]),
                ("Tool".to_owned(), vec!["a".to_owned()]),
                ("Tool".to_owned(), vec!["b".to_owned()]),
                ("Assistant".to_owned(), vec!["c".to_owned()]),
                ("Tool".to_owned(), vec!["c".to_owned()]),
            ]
        );
        let results = looped.sent[2]
            .messages
            .iter()
            .filter_map(|message| match (&message.tool_call_id, &message.content) {
                (Some(_), Some(MessageContentWire::Text(text))) => Some(text.as_str()),
                _ => None,
            })
            .collect::<Vec<_>>();
        assert_eq!(results, [r#"{"sum":3}"#, r#"{"sum":7}"#, r#"{"sum":11}"#]);
        assert_eq!(
            evidence(&handle, run.job)
                .iter()
                .map(|record| (
                    record.call,
                    record.tool_call_id.as_str(),
                    outcome_of(record)
                ))
                .collect::<Vec<_>>(),
            [
                (
                    tool_call(run.job, 1, 0),
                    "a",
                    Some(completed(r#"{"sum":3}"#))
                ),
                (
                    tool_call(run.job, 1, 1),
                    "b",
                    Some(completed(r#"{"sum":7}"#))
                ),
                (
                    tool_call(run.job, 2, 0),
                    "c",
                    Some(completed(r#"{"sum":11}"#))
                ),
            ],
            "each call's evidence is its own, under its request"
        );
        assert_eq!(
            by_request(&handle, run.job)
                .keys()
                .copied()
                .collect::<Vec<_>>(),
            [1, 2, 3]
        );
        assert!(job_record(&handle, run.job).completed_at.is_some());
        worker.shutdown().expect("shutdown");
    }

    #[test]
    fn calls_refused_at_dispatch_never_run_and_are_no_provider_failure() {
        let (_root, directory) = data_directory("tool-loop-refusals");
        let worker = DatabaseWorker::start(&directory).expect("start database");
        let handle = worker.handle();
        let (_provider, llm) = llm_setup(&handle, "model-a");
        type Change = Box<dyn FnMut(&ToolRegistry)>;
        let cases: Vec<(&str, Change, ToolCallRefusal)> = vec![
            (
                "a grant withdrawn before dispatch",
                Box::new(|registry: &ToolRegistry| registry.revoke("add").expect("revoke")),
                ToolCallRefusal::Unauthorized,
            ),
            (
                "a tool registered again before dispatch",
                Box::new(|registry: &ToolRegistry| {
                    registry.unregister("add").expect("unregister");
                    registry
                        .register(
                            "add",
                            None,
                            add_schema(),
                            Arc::new(TestTool {
                                behavior: Behavior::Add,
                                calls: Arc::new(AtomicUsize::new(0)),
                                effect: ToolEffect::ReadOnly,
                            }),
                        )
                        .expect("register again");
                    registry.authorize("add").expect("authorize");
                }),
                ToolCallRefusal::Unauthorized,
            ),
        ];
        for (case, mut change, kind) in cases {
            let run = local_run(&handle, llm);
            let (registry, calls) = registered(vec![("add", Behavior::Add)]);
            let tools = execution(&registry, &["add"], ToolLoopLimits::default());
            let changed = registry.clone();
            let looped = run_loop(
                &handle,
                run,
                Some(tools),
                vec![calling(&[add(0, "c", 1, 2)], (1, 1))],
                CancellationToken::default(),
                move |_| change(&changed),
            );
            let (receipt, stop) = stopped(looped);
            assert!(
                matches!(&stop, ToolLoopStop::Refused(refusal) if refusal.kind == kind),
                "{case}: {stop:?}"
            );
            assert_eq!(calls.load(Ordering::SeqCst), 0, "{case}: nothing ran");
            assert!(receipt.result.is_some());
            let job = job_record(&handle, run.job);
            assert!(
                job.failed_at.is_some() && job.completed_at.is_none(),
                "{case}"
            );
            assert!(
                stats(&handle, run.job)
                    .iter()
                    .all(|stat| stat.observation.failure.is_none()),
                "{case}: a local refusal is no Provider failure"
            );
            assert!(blocking(&handle, llm).is_empty(), "{case}");
        }

        // A grant withdrawn between two calls of one response.
        let run = local_run(&handle, llm);
        let registry = ToolRegistry::default();
        let calls = Arc::new(AtomicUsize::new(0));
        for (name, behavior) in [
            ("revoker", Behavior::Revoke(registry.clone(), "add")),
            ("add", Behavior::Add),
        ] {
            registry
                .register(
                    name,
                    None,
                    add_schema(),
                    Arc::new(TestTool {
                        behavior,
                        calls: calls.clone(),
                        effect: ToolEffect::ReadOnly,
                    }),
                )
                .expect("register");
            registry.authorize(name).expect("authorize");
        }
        let looped = run_loop(
            &handle,
            run,
            Some(execution(
                &registry,
                &["revoker", "add"],
                ToolLoopLimits::default(),
            )),
            vec![calling(
                &[
                    call(0, "r", "revoker", r#"{"a":0,"b":0}"#),
                    add(1, "c", 1, 2),
                ],
                (1, 1),
            )],
            CancellationToken::default(),
            |_| {},
        );
        let (_, stop) = stopped(looped);
        assert!(matches!(stop, ToolLoopStop::Refused(ref refusal) if refusal.index == Some(1)));
        assert_eq!(
            calls.load(Ordering::SeqCst),
            1,
            "only the call before the withdrawal ran"
        );

        // An id that repeats an earlier call's.
        let run = local_run(&handle, llm);
        let (registry, calls) = registered(vec![("add", Behavior::Add)]);
        let looped = run_loop(
            &handle,
            run,
            Some(execution(&registry, &["add"], ToolLoopLimits::default())),
            vec![
                calling(&[add(0, "same", 1, 2)], (1, 1)),
                calling(&[add(0, "same", 3, 4)], (1, 1)),
            ],
            CancellationToken::default(),
            |_| {},
        );
        let (_, stop) = stopped(looped);
        assert!(matches!(
            stop,
            ToolLoopStop::Refused(ref refusal) if refusal.kind == ToolCallRefusal::ConflictingIdentity
        ));
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        worker.shutdown().expect("shutdown");
    }

    #[test]
    fn calls_the_request_did_not_admit_never_reach_a_handler() {
        let (_root, directory) = data_directory("tool-loop-admission");
        let worker = DatabaseWorker::start(&directory).expect("start database");
        let handle = worker.handle();
        let (_provider, llm) = llm_setup(&handle, "model-a");
        let ambiguous = r#"{"index":0,"id":"c","function":{"name":"add","arguments":"{\"a\":1,\"b\":2}"}},{"index":0,"function":{"name":"subtract"}}"#;
        for (case, calls_sent) in [
            (
                "an unregistered tool",
                vec![call(0, "c", "subtract", r#"{"a":1,"b":2}"#)],
            ),
            (
                "arguments the schema refuses",
                vec![call(0, "c", "add", r#"{"a":"1","b":2}"#)],
            ),
            (
                "an unexpected argument",
                vec![call(0, "c", "add", r#"{"a":1,"b":2,"c":3}"#)],
            ),
            ("an ambiguous name", vec![ambiguous.to_owned()]),
        ] {
            let run = local_run(&handle, llm);
            let (registry, calls) = registered(vec![("add", Behavior::Add)]);
            let looped = run_loop(
                &handle,
                run,
                Some(execution(&registry, &["add"], ToolLoopLimits::default())),
                vec![calling(&calls_sent, (1, 1))],
                CancellationToken::default(),
                |_| {},
            );
            let receipt = looped.settled.expect("the refused response settles");
            assert_eq!(receipt.result, None, "{case}");
            assert_eq!(calls.load(Ordering::SeqCst), 0, "{case}: nothing ran");
            let job = job_record(&handle, run.job);
            assert!(
                job.unknown_at.is_some()
                    && job.stop_confirmed_at.is_some()
                    && job.completed_at.is_none(),
                "{case}: an inadmissible response stays an unknown outcome, as before"
            );
            assert_eq!(looped.sent.len(), 1, "{case}: nothing was sent back");
        }
        worker.shutdown().expect("shutdown");
    }

    #[test]
    fn handler_failures_and_limits_stop_the_loop_without_completing_the_job() {
        let (_root, directory) = data_directory("tool-loop-limits");
        let worker = DatabaseWorker::start(&directory).expect("start database");
        let handle = worker.handle();
        let (_provider, llm) = llm_setup(&handle, "model-a");
        let limits = |change: fn(&mut ToolLoopLimits)| {
            let mut limits = ToolLoopLimits::default();
            change(&mut limits);
            limits
        };
        let one = || vec![calling(&[add(0, "c", 1, 2)], (1, 1))];
        /// A case, its tool, its limits, its responses, the stop and the calls that ran.
        type Case = (
            &'static str,
            Behavior,
            ToolLoopLimits,
            Vec<Vec<Step>>,
            ToolLoopStop,
            usize,
        );
        let cases: Vec<Case> = vec![
            (
                "a failing handler",
                Behavior::Fail,
                ToolLoopLimits::default(),
                one(),
                ToolLoopStop::HandlerFailed {
                    index: 0,
                    message: "the tool failed".into(),
                },
                1,
            ),
            (
                "a result over its limit",
                Behavior::Big(65),
                limits(|limits| limits.max_tool_result_bytes = 66),
                one(),
                ToolLoopStop::ResultTooLarge { index: 0 },
                1,
            ),
            (
                "results over their accumulated limit",
                Behavior::Add,
                limits(|limits| limits.max_tool_results_bytes = 20),
                vec![
                    calling(&[add(0, "a", 1, 2), add(1, "b", 1, 2)], (1, 1)),
                    calling(&[add(0, "c", 1, 2)], (1, 1)),
                ],
                ToolLoopStop::ResultTooLarge { index: 0 },
                3,
            ),
            (
                "tools requested in the last allowed request",
                Behavior::Add,
                limits(|limits| limits.max_provider_requests = 2),
                vec![
                    calling(&[add(0, "a", 1, 2)], (1, 1)),
                    calling(&[add(0, "b", 1, 2)], (1, 1)),
                ],
                ToolLoopStop::RequestLimit,
                1,
            ),
            (
                "more calls than allowed",
                Behavior::Add,
                limits(|limits| limits.max_tool_calls = 2),
                vec![calling(
                    &[add(0, "a", 1, 2), add(1, "b", 1, 2), add(2, "c", 1, 2)],
                    (1, 1),
                )],
                ToolLoopStop::CallLimit,
                0,
            ),
        ];
        for (case, behavior, limits, responses, expected, ran) in cases {
            let run = local_run(&handle, llm);
            let (registry, calls) = registered(vec![("add", behavior)]);
            let looped = run_loop(
                &handle,
                run,
                Some(execution(&registry, &["add"], limits)),
                responses,
                CancellationToken::default(),
                |_| {},
            );
            let (receipt, stop) = stopped(looped);
            assert_eq!(stop, expected, "{case}");
            assert_eq!(calls.load(Ordering::SeqCst), ran, "{case}");
            assert!(receipt.result.is_some(), "{case}: the failure is published");
            let job = job_record(&handle, run.job);
            assert!(
                job.failed_at.is_some() && job.completed_at.is_none(),
                "{case}"
            );
            assert!(blocking(&handle, llm).is_empty(), "{case}");
            assert!(
                stats(&handle, run.job)
                    .iter()
                    .all(|stat| stat.observation.failure.is_none()),
                "{case}: no Provider failure"
            );
        }

        // At their limits the loop still finishes.
        for (limits, responses) in [
            (
                limits(|limits| limits.max_provider_requests = 2),
                vec![
                    calling(&[add(0, "a", 1, 2)], (1, 1)),
                    answering("3", (1, 1)),
                ],
            ),
            (
                limits(|limits| limits.max_tool_calls = 2),
                vec![
                    calling(&[add(0, "a", 1, 2), add(1, "b", 1, 2)], (1, 1)),
                    answering("3 and 3", (1, 1)),
                ],
            ),
        ] {
            let run = local_run(&handle, llm);
            let (registry, _) = registered(vec![("add", Behavior::Add)]);
            let looped = run_loop(
                &handle,
                run,
                Some(execution(&registry, &["add"], limits)),
                responses,
                CancellationToken::default(),
                |_| {},
            );
            looped.settled.expect("a loop within its limits completes");
            assert!(job_record(&handle, run.job).completed_at.is_some());
        }
        worker.shutdown().expect("shutdown");
    }

    #[test]
    fn a_stopped_handler_leaves_the_outcome_unknown_and_its_stop_confirmed() {
        let (_root, directory) = data_directory("tool-loop-cancel");
        let worker = DatabaseWorker::start(&directory).expect("start database");
        let handle = worker.handle();
        let (_provider, llm) = llm_setup(&handle, "model-a");
        let token = CancellationToken::default();
        let cases: Vec<(
            &str,
            Behavior,
            ToolLoopLimits,
            CancellationToken,
            bool,
            ToolLoopStop,
            usize,
        )> = vec![
            (
                "cancelled before dispatch",
                Behavior::Add,
                ToolLoopLimits::default(),
                CancellationToken::default(),
                true,
                ToolLoopStop::Cancelled,
                0,
            ),
            (
                "cancelled while the handler runs",
                Behavior::CancelThenHang(token.clone()),
                ToolLoopLimits::default(),
                token,
                false,
                ToolLoopStop::Cancelled,
                1,
            ),
        ];
        for (case, behavior, limits, cancellation, cancel_after_read, expected, ran) in cases {
            let run = local_run(&handle, llm);
            let (registry, calls) = registered(vec![("add", behavior)]);
            let canceller = cancellation.clone();
            let looped = run_loop(
                &handle,
                run,
                Some(execution(&registry, &["add"], limits)),
                vec![calling(&[add(0, "c", 1, 2)], (1, 1))],
                cancellation,
                move |_| {
                    if cancel_after_read {
                        canceller.cancel();
                    }
                },
            );
            let (receipt, stop) = stopped(looped);
            assert_eq!(stop, expected, "{case}");
            assert_eq!(calls.load(Ordering::SeqCst), ran, "{case}");
            assert_eq!(
                receipt.result, None,
                "{case}: an unknown outcome publishes nothing"
            );
            let job = job_record(&handle, run.job);
            assert!(
                job.unknown_at.is_some() && job.stop_confirmed_at.is_some(),
                "{case}"
            );
            assert!(
                job.completed_at.is_none() && job.failed_at.is_none(),
                "{case}"
            );
            assert!(blocking(&handle, llm).is_empty(), "{case}");
            let records = evidence(&handle, run.job);
            if ran == 0 {
                assert!(records.is_empty(), "{case}: nothing was admitted");
            } else {
                assert_eq!(
                    records.iter().map(outcome_of).collect::<Vec<_>>(),
                    [Some(ToolOutcome::Uncertain {
                        cause: UncertainCause::Cancelled,
                        message: None,
                    })],
                    "{case}"
                );
                assert!(
                    records[0].is_unresolved(),
                    "{case}: the stop resolves nothing"
                );
            }
        }
        worker.shutdown().expect("shutdown");
    }

    /// Alone in its test: ntex runs the timers of only the first System a
    /// thread creates, so this must be that System.
    #[test]
    fn a_handler_past_its_time_limit_is_stopped_and_its_outcome_unknown() {
        let (_root, directory) = data_directory("tool-loop-timeout");
        let worker = DatabaseWorker::start(&directory).expect("start database");
        let handle = worker.handle();
        let (_provider, llm) = llm_setup(&handle, "model-a");
        let run = local_run(&handle, llm);
        let (registry, calls) = registered(vec![("add", Behavior::Hang)]);
        let limits = ToolLoopLimits {
            handler_timeout: Duration::from_millis(50),
            ..ToolLoopLimits::default()
        };
        let looped = run_loop(
            &handle,
            run,
            Some(execution(&registry, &["add"], limits)),
            vec![calling(&[add(0, "c", 1, 2)], (1, 1))],
            CancellationToken::default(),
            |_| {},
        );
        let (receipt, stop) = stopped(looped);
        assert_eq!(stop, ToolLoopStop::TimedOut { index: 0 });
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        assert_eq!(receipt.result, None);
        let job = job_record(&handle, run.job);
        assert!(job.unknown_at.is_some() && job.stop_confirmed_at.is_some());
        assert!(blocking(&handle, llm).is_empty());
        let records = evidence(&handle, run.job);
        assert_eq!(
            records.iter().map(outcome_of).collect::<Vec<_>>(),
            [Some(ToolOutcome::Uncertain {
                cause: UncertainCause::TimedOut,
                message: None,
            })]
        );
        assert!(
            records[0].is_unresolved(),
            "a stopped handler's effect stays open"
        );
        worker.shutdown().expect("shutdown");
    }

    fn add_tool() -> Arc<TestTool> {
        Arc::new(TestTool {
            behavior: Behavior::Add,
            calls: Arc::new(AtomicUsize::new(0)),
            effect: ToolEffect::ReadOnly,
        })
    }

    #[test]
    fn the_registry_grants_nothing_unasked_and_never_to_a_new_registration() {
        use crate::openai_compatible::CompletedToolCall;
        use crate::orchestration::tools::ToolRegistryError;

        let registry = ToolRegistry::default();
        assert_eq!(
            registry.offer(&["add"]).map(|_| ()),
            Err(ToolRegistryError::Unregistered("add".into())),
            "a registry starts empty"
        );
        assert!(
            registry
                .offer(&[])
                .expect("no tools")
                .wires()
                .expect("none")
                .is_empty()
        );
        registry
            .register("add", None, add_schema(), add_tool())
            .expect("register");
        assert_eq!(
            registry.offer(&["add"]).map(|_| ()),
            Err(ToolRegistryError::Unauthorized("add".into())),
            "registering grants nothing"
        );
        registry.authorize("add").expect("authorize");
        let offer = registry.offer(&["add"]).expect("an authorized tool");
        let call = CompletedToolCall {
            index: 0,
            id: "c".into(),
            name: "add".into(),
            arguments: r#"{"a":1,"b":2}"#.into(),
        };
        assert!(offer.resolve(&call).is_ok());

        registry.unregister("add").expect("unregister");
        registry
            .register("add", None, add_schema(), add_tool())
            .expect("register again");
        assert_eq!(
            registry.offer(&["add"]).map(|_| ()),
            Err(ToolRegistryError::Unauthorized("add".into())),
            "the new registration does not inherit the old grant"
        );
        registry.authorize("add").expect("authorize the new one");
        assert_eq!(
            offer.wires().map(|_| ()),
            Err(ToolRegistryError::Changed("add".into()))
        );
        let refused = offer
            .resolve(&call)
            .map(|_| ())
            .expect_err("another generation");
        assert_eq!(refused.kind, ToolCallRefusal::Unauthorized);
        assert!(
            refused.message.contains("registered again"),
            "{}",
            refused.message
        );
    }

    #[test]
    fn a_handler_admitted_before_its_revocation_finishes_and_no_later_call_is_admitted() {
        let (_root, directory) = data_directory("tool-loop-revocation");
        let worker = DatabaseWorker::start(&directory).expect("start database");
        let handle = worker.handle();
        let (_provider, llm) = llm_setup(&handle, "model-a");
        let run = local_run(&handle, llm);
        let registry = ToolRegistry::default();
        let calls = Arc::new(AtomicUsize::new(0));
        // The handler withdraws its own grant while it runs.
        registry
            .register(
                "add",
                None,
                add_schema(),
                Arc::new(TestTool {
                    behavior: Behavior::Revoke(registry.clone(), "add"),
                    calls: calls.clone(),
                    effect: ToolEffect::ReadOnly,
                }),
            )
            .expect("register");
        registry.authorize("add").expect("authorize");
        let looped = run_loop(
            &handle,
            run,
            Some(execution(&registry, &["add"], ToolLoopLimits::default())),
            vec![
                calling(&[add(0, "first", 1, 2)], (1, 1)),
                calling(&[add(0, "second", 1, 2)], (1, 1)),
            ],
            CancellationToken::default(),
            |_| {},
        );
        assert_eq!(looped.sent.len(), 2, "the admitted call's result was sent");
        let answered = looped.sent[1].messages.iter().find_map(|message| {
            match (&message.tool_call_id, &message.content) {
                (Some(id), Some(MessageContentWire::Text(text))) if id == "first" => {
                    Some(text.clone())
                }
                _ => None,
            }
        });
        assert_eq!(answered.as_deref(), Some(r#"{"revoked":"add"}"#));
        let (_, stop) = stopped(looped);
        assert!(
            matches!(&stop, ToolLoopStop::Refused(refusal)
                if refusal.kind == ToolCallRefusal::Unauthorized && refusal.index == Some(0)),
            "{stop:?}"
        );
        assert_eq!(
            calls.load(Ordering::SeqCst),
            1,
            "no call was admitted after the revocation"
        );
        let records = evidence(&handle, run.job);
        assert_eq!(
            records
                .iter()
                .map(|record| (
                    record.call,
                    record.decision.fact.clone(),
                    outcome_of(record)
                ))
                .collect::<Vec<_>>(),
            [
                (
                    tool_call(run.job, 1, 0),
                    records[0].decision.fact.clone(),
                    Some(completed(r#"{"revoked":"add"}"#))
                ),
                (
                    tool_call(run.job, 2, 0),
                    ToolDecision::Refused(ToolCallRefusal::Unauthorized),
                    None
                ),
            ],
            "the call that passed its final check completed; the later one was refused"
        );
        assert!(records[0].admission().is_some());
        worker.shutdown().expect("shutdown");
    }

    #[test]
    fn limits_that_allow_no_request_are_refused_before_anything_is_sent() {
        use crate::orchestration::execution_runtime::ToolLimitsError;

        assert_eq!(ToolLoopLimits::default().validate(), Ok(()));
        let zero_requests = ToolLoopLimits {
            max_provider_requests: 0,
            ..ToolLoopLimits::default()
        };
        let zero_time = ToolLoopLimits {
            handler_timeout: Duration::ZERO,
            ..ToolLoopLimits::default()
        };
        assert_eq!(
            zero_requests.validate(),
            Err(ToolLimitsError::NoProviderRequest)
        );
        assert_eq!(zero_time.validate(), Err(ToolLimitsError::NoHandlerTime));
        for refusing in [
            ToolLoopLimits {
                max_tool_calls: 0,
                ..ToolLoopLimits::default()
            },
            ToolLoopLimits {
                max_tool_result_bytes: 0,
                max_tool_results_bytes: 0,
                ..ToolLoopLimits::default()
            },
        ] {
            assert_eq!(
                refusing.validate(),
                Ok(()),
                "an explicit refusal stays valid"
            );
        }

        let (_root, directory) = data_directory("tool-loop-invalid-limits");
        let worker = DatabaseWorker::start(&directory).expect("start database");
        let handle = worker.handle();
        let (_provider, llm) = llm_setup(&handle, "model-a");
        let (registry, calls) = registered(vec![("add", Behavior::Add)]);

        // Through the runtime: refused before anything is persisted.
        let executor = executor(&handle, "llm worker", &[OPENAI]).expect("register");
        let markers = || wait(handle.jobs(None, 1_000)).expect("Jobs").len();
        let before = markers();
        let runtime = ExecutionRuntime::start(handle.clone()).expect("start runtime");
        let mut plan = ProviderExecutionPlan::environment_credentials(
            executor,
            JobInputDraft::default(),
            NewJob {
                configuration: llm_only(llm),
                ..new_job(&handle, "tool plan", handle.genesis(), Vec::new())
            },
        );
        plan.tools = Some(execution(&registry, &["add"], zero_requests));
        assert!(matches!(
            wait(runtime.scheduler().schedule_provider(plan)),
            Err(ExecutionRuntimeError::InvalidToolLimits(
                ToolLimitsError::NoProviderRequest
            ))
        ));
        wait(runtime.shutdown()).expect("shutdown runtime");
        assert_eq!(markers(), before, "no input, Job or evidence was written");

        // Called on a Job already started: it fails without a request.
        for (limits, expected) in [
            (zero_requests, ToolLimitsError::NoProviderRequest),
            (zero_time, ToolLimitsError::NoHandlerTime),
        ] {
            let run = local_run(&handle, llm);
            let looped = run_loop(
                &handle,
                run,
                Some(execution(&registry, &["add"], limits)),
                Vec::new(),
                CancellationToken::default(),
                |_| {},
            );
            assert!(looped.sent.is_empty(), "{expected}: no request was sent");
            let (receipt, stop) = stopped(looped);
            assert_eq!(stop, ToolLoopStop::InvalidLimits(expected));
            assert!(
                receipt.result.is_some(),
                "{expected}: the failure is published"
            );
            let job = job_record(&handle, run.job);
            assert!(
                job.failed_at.is_some() && job.completed_at.is_none(),
                "{expected}"
            );
            assert!(
                job_metrics(&handle, run.job).is_empty() && stats(&handle, run.job).is_empty(),
                "{expected}: no Provider fact is made up"
            );
            let outputs = wait(handle.job_outputs(run.job, None, 10)).expect("outputs");
            assert_eq!(outputs.last().and_then(|output| output.value.clone()), None);
        }
        assert_eq!(calls.load(Ordering::SeqCst), 0);
        assert!(blocking(&handle, llm).is_empty(), "no Profile stays held");
        worker.shutdown().expect("shutdown");
        let worker = DatabaseWorker::start(&directory).expect("restart");
        assert!(
            worker.handle().abandoned_at_open().is_empty(),
            "no local claim stays behind"
        );
        worker.shutdown().expect("shutdown");
    }

    #[test]
    fn one_allowed_request_is_exactly_one() {
        let (_root, directory) = data_directory("tool-loop-one-request");
        let worker = DatabaseWorker::start(&directory).expect("start database");
        let handle = worker.handle();
        let (_provider, llm) = llm_setup(&handle, "model-a");
        let one = ToolLoopLimits {
            max_provider_requests: 1,
            ..ToolLoopLimits::default()
        };
        let (registry, calls) = registered(vec![("add", Behavior::Add)]);

        let run = local_run(&handle, llm);
        let looped = run_loop(
            &handle,
            run,
            Some(execution(&registry, &["add"], one)),
            vec![answering("no tools needed", (1, 1))],
            CancellationToken::default(),
            |_| {},
        );
        assert_eq!(looped.sent.len(), 1);
        looped.settled.expect("one request answers");
        assert!(job_record(&handle, run.job).completed_at.is_some());

        let run = local_run(&handle, llm);
        let looped = run_loop(
            &handle,
            run,
            Some(execution(&registry, &["add"], one)),
            vec![calling(&[add(0, "c", 1, 2)], (1, 1))],
            CancellationToken::default(),
            |_| {},
        );
        assert_eq!(
            looped.sent.len(),
            1,
            "a response asking for tools sends no second request"
        );
        let (_, stop) = stopped(looped);
        assert_eq!(stop, ToolLoopStop::RequestLimit);
        assert_eq!(
            calls.load(Ordering::SeqCst),
            0,
            "no tool ran for a request never sent"
        );
        assert!(job_record(&handle, run.job).failed_at.is_some());
        worker.shutdown().expect("shutdown");
    }

    #[test]
    fn a_failed_settlement_of_invalid_limits_is_reported_and_claims_no_outcome() {
        use crate::disk_protection::{
            CleanupConfig, FilesystemConfig, ProtectedArea, ProtectionReason, SpaceShortage,
        };

        let (_root, directory) = data_directory("tool-loop-invalid-unsettled");
        let lenient = FilesystemConfig {
            disk_lock_threshold_bytes: 1,
            disk_resume_threshold_bytes: 2,
            disk_check_interval_ms: 3_600_000,
            cleanup_config: CleanupConfig::default(),
        };
        let worker = DatabaseWorker::start_with_config(&directory, lenient).expect("start");
        let handle = worker.handle();
        let (_provider, llm) = llm_setup(&handle, "model-a");
        let (registry, _) = registered(vec![("add", Behavior::Add)]);
        let run = local_run(&handle, llm);
        assert!(
            handle.activate_filesystem_protection(ProtectionReason::InsufficientSpace(
                SpaceShortage {
                    area: ProtectedArea::Database,
                    path: directory.database_path().to_path_buf(),
                    available_bytes: 0,
                    expected_write_bytes: 0,
                    threshold_bytes: 1,
                }
            ))
        );
        let looped = run_loop(
            &handle,
            run,
            Some(execution(
                &registry,
                &["add"],
                ToolLoopLimits {
                    max_provider_requests: 0,
                    ..ToolLoopLimits::default()
                },
            )),
            Vec::new(),
            CancellationToken::default(),
            |_| {},
        );
        assert!(looped.sent.is_empty());
        match looped.settled {
            Err(ExecutionRuntimeError::ToolLoopNotSettled {
                stop,
                source,
                evidence: None,
                observations: None,
            }) => {
                assert_eq!(
                    *stop,
                    ToolLoopStop::InvalidLimits(
                        crate::orchestration::execution_runtime::ToolLimitsError::NoProviderRequest
                    ),
                    "the stop is reported with the failed settlement"
                );
                assert!(
                    !matches!(source, DatabaseError::Rejected(_)),
                    "storage refused it, not a rule: {source:?}"
                );
            }
            other => panic!("the failed settlement must be reported, got {other:?}"),
        }
        wait(handle.recover_storage()).expect("recover");
        let job = job_record(&handle, run.job);
        assert!(
            job.failed_at.is_none() && job.completed_at.is_none() && job.unknown_at.is_none(),
            "no outcome was claimed"
        );
        assert!(job_metrics(&handle, run.job).is_empty() && stats(&handle, run.job).is_empty());
        worker.shutdown().expect("shutdown");
    }

    #[test]
    fn without_tools_one_request_settles_as_before() {
        let (_root, directory) = data_directory("tool-loop-none");
        let worker = DatabaseWorker::start(&directory).expect("start database");
        let handle = worker.handle();
        let (_provider, llm) = llm_setup(&handle, "model-a");
        let run = local_run(&handle, llm);
        let looped = run_loop(
            &handle,
            run,
            None,
            vec![answering("hello", (2, 1))],
            CancellationToken::default(),
            |_| {},
        );
        looped.settled.expect("settles");
        assert_eq!(looped.sent.len(), 1);
        assert!(
            looped.sent[0].tools.is_empty(),
            "no tool is ever offered by default"
        );
        assert!(job_record(&handle, run.job).completed_at.is_some());
        assert_eq!(
            by_request(&handle, run.job)
                .keys()
                .copied()
                .collect::<Vec<_>>(),
            [1]
        );
        worker.shutdown().expect("shutdown");
    }

    #[test]
    fn the_runtime_offers_only_tools_still_authorized_and_persists_nothing_otherwise() {
        let (_root, directory) = data_directory("tool-loop-offer");
        let worker = DatabaseWorker::start(&directory).expect("start database");
        let handle = worker.handle();
        let (_provider, llm) = llm_setup(&handle, "model-a");
        let executor = executor(&handle, "llm worker", &[OPENAI]).expect("register");
        let (registry, calls) = registered(vec![("add", Behavior::Add)]);
        let offer = registry.offer(&["add"]).expect("offer");
        registry.revoke("add").expect("revoke");
        let jobs = wait(handle.jobs(None, 1_000)).expect("jobs").len();
        let runtime = ExecutionRuntime::start(handle.clone()).expect("start runtime");
        let mut plan = ProviderExecutionPlan::environment_credentials(
            executor,
            JobInputDraft::default(),
            NewJob {
                configuration: llm_only(llm),
                ..new_job(&handle, "tool plan", handle.genesis(), Vec::new())
            },
        );
        assert!(
            plan.tools.is_none(),
            "a plan offers no tool unless OCG selects one"
        );
        plan.tools = Some(ToolExecution {
            offer,
            limits: ToolLoopLimits::default(),
        });
        assert!(matches!(
            wait(runtime.scheduler().schedule_provider(plan)),
            Err(ExecutionRuntimeError::ToolsUnavailable(_))
        ));
        assert_eq!(wait(handle.jobs(None, 1_000)).expect("jobs").len(), jobs);
        assert_eq!(calls.load(Ordering::SeqCst), 0);
        wait(runtime.shutdown()).expect("shutdown runtime");
        assert!(matches!(
            registry.register(
                "add",
                None,
                add_schema(),
                Arc::new(TestTool {
                    behavior: Behavior::Add,
                    calls: Arc::new(AtomicUsize::new(0)),
                    effect: ToolEffect::ReadOnly,
                })
            ),
            Err(crate::orchestration::tools::ToolRegistryError::Duplicate(_))
        ));
        assert!(matches!(
            registry.register(
                "open",
                None,
                json!({"type": "object", "requird": []}),
                Arc::new(TestTool {
                    behavior: Behavior::Add,
                    calls: Arc::new(AtomicUsize::new(0)),
                    effect: ToolEffect::ReadOnly,
                })
            ),
            Err(crate::orchestration::tools::ToolRegistryError::UnsupportedSchema(..))
        ));
        assert!(matches!(
            registry.offer(&["add"]),
            Err(crate::orchestration::tools::ToolRegistryError::Unauthorized(_))
        ));
        worker.shutdown().expect("shutdown");
    }

    // ---- Durable tool evidence ------------------------------------------------

    use crate::disk_protection::{
        CleanupConfig, FilesystemConfig, ProtectedArea, ProtectionReason, SpaceShortage,
    };
    use crate::orchestration::execution_runtime::dispatch_seam::{self, Point};
    use crate::persistence::{
        AuthorizationBasis, EffectConclusion, MAX_STORED_TOOL_ARGUMENTS_BYTES,
        MAX_STORED_TOOL_ARGUMENTS_PER_JOB_BYTES, NewToolEvidence, NotInvokedReason, ToolAdmission,
        ToolCallRef, ToolDecision, ToolEvidenceRecord, ToolOutcome, ToolOutcomeKind, ToolPayload,
        ToolReconciliation, UncertainCause, WithheldReason,
    };

    fn evidence(handle: &DatabaseHandle, job: JobId) -> Vec<ToolEvidenceRecord> {
        wait(handle.tool_evidence(job, None, 1_000)).expect("read tool evidence")
    }

    fn tool_call(job: JobId, request: u32, index: u32) -> ToolCallRef {
        ToolCallRef {
            job,
            request: number(request),
            index,
        }
    }

    /// A Job this process executes, whose first `requests` Provider requests
    /// were delivered.
    fn observed_run(handle: &DatabaseHandle, llm: ProfileId, requests: u32) -> TokenRun {
        let run = local_run(handle, llm);
        for request in 1..=requests {
            wait(handle.record_metrics(NewMetrics {
                job: run.job,
                request: number(request),
                observed_at: Timestamp::now(),
                measurements: Measurements::default(),
                token: Some(TokenAnnotation::eligible(UsageFinality::Cumulative)),
            }))
            .expect("record the delivery");
        }
        run
    }

    fn admitted_with(
        call: ToolCallRef,
        id: &str,
        effect: ToolEffect,
        arguments: &str,
    ) -> NewToolEvidence {
        NewToolEvidence {
            call,
            tool_call_id: id.into(),
            name: "add".into(),
            decision: ToolDecision::Admitted(ToolAdmission {
                handler_version: "test-1".into(),
                effect,
                parameters: add_schema().to_string(),
                arguments: ToolPayload::new(arguments),
                authorization: AuthorizationBasis::RegistryGrant,
            }),
            spawns: false,
        }
    }

    fn admitted(call: ToolCallRef, id: &str, effect: ToolEffect) -> NewToolEvidence {
        admitted_with(call, id, effect, r#"{"a":1,"b":2}"#)
    }

    fn refused(call: ToolCallRef, id: &str) -> NewToolEvidence {
        NewToolEvidence {
            call,
            tool_call_id: id.into(),
            name: "add".into(),
            decision: ToolDecision::Refused(ToolCallRefusal::Unauthorized),
            spawns: false,
        }
    }

    fn completed(result: &str) -> ToolOutcome {
        ToolOutcome::Completed {
            result: ToolPayload::new(result),
        }
    }

    fn uncertain() -> ToolOutcome {
        ToolOutcome::Uncertain {
            cause: UncertainCause::HandlerReported,
            message: Some("the connection dropped".into()),
        }
    }

    fn verified(conclusion: EffectConclusion) -> ToolReconciliation {
        ToolReconciliation {
            conclusion,
            reconciler: "test reconciler".into(),
            evidence: "external record 42".into(),
            explanation: "the external system was checked".into(),
        }
    }

    fn outcome_of(record: &ToolEvidenceRecord) -> Option<ToolOutcome> {
        record.outcome.as_ref().map(|outcome| outcome.fact.clone())
    }

    fn settle_unknown(handle: &DatabaseHandle, job: JobId) {
        wait(handle.finalize_job(job, JobFinalization::Unknown)).expect("an unknown outcome");
        // No admitted call here can spawn, so the stop is confirmed at once.
        assert!(
            wait(handle.job(job))
                .expect("read")
                .expect("exists")
                .stop_confirmed_at
                .is_some()
        );
    }

    fn retry_of(original: JobId, llm: ProfileId, acknowledge_repeated_effects: bool) -> JobRetry {
        let _ = llm;
        JobRetry {
            original,
            parent: JobId::new(0),
            name: Some("retry".into()),
            eligible_at: None,
            projects: Vec::new(),
            dependencies: Vec::new(),
            acknowledge_repeated_effects,
        }
    }

    /// A configuration whose disk checks never lock, so only the protection
    /// a test activates refuses commands.
    fn lenient() -> FilesystemConfig {
        FilesystemConfig {
            disk_lock_threshold_bytes: 1,
            disk_resume_threshold_bytes: 2,
            disk_check_interval_ms: 3_600_000,
            cleanup_config: CleanupConfig::default(),
        }
    }

    /// Makes storage refuse every command from now on, as a full disk would.
    fn protect(handle: &DatabaseHandle, directory: &std::path::Path) {
        assert!(
            handle.activate_filesystem_protection(ProtectionReason::InsufficientSpace(
                SpaceShortage {
                    area: ProtectedArea::Database,
                    path: directory.to_path_buf(),
                    available_bytes: 0,
                    expected_write_bytes: 0,
                    threshold_bytes: 1,
                }
            ))
        );
    }

    /// Makes the commit of the next command fail after it may have become
    /// durable: the command is held as the first of a group, a write of the
    /// Job's joins the group, and the group's durable flush fails. Returns
    /// what that companion write was answered.
    fn fail_next_commit(
        handle: &DatabaseHandle,
        job: JobId,
    ) -> std::thread::JoinHandle<Result<crate::persistence::MetricsId, DatabaseError>> {
        let handle = handle.clone();
        let (held, release) = handle.group_probe().hold_next_group();
        std::thread::spawn(move || {
            held.recv_timeout(crate::test_support::WAIT)
                .expect("the next command is held");
            let companion = admit(handle.record_metrics(NewMetrics {
                job,
                request: number(1),
                observed_at: Timestamp::now(),
                measurements: Measurements::default(),
                token: Some(TokenAnnotation::eligible(UsageFinality::Cumulative)),
            }));
            handle
                .group_probe()
                .fail_next_flush
                .store(true, Ordering::SeqCst);
            release.send(()).expect("release the group");
            companion()
        })
    }

    /// The tool message that answered `id` in a sent conversation.
    fn answer_to(conversation: &ChatConversation, id: &str) -> Option<String> {
        conversation.messages.iter().find_map(|message| {
            match (&message.tool_call_id, &message.content) {
                (Some(answered), Some(MessageContentWire::Text(text))) if answered == id => {
                    Some(text.clone())
                }
                _ => None,
            }
        })
    }

    /// A response that requests `calls`, each with its arguments sent in
    /// fragments small enough for one stream line each.
    fn calling_in_fragments(calls: &[(u32, &str, &str, String)]) -> Vec<Step> {
        let delta = |delta: String| {
            chunk(format!(
                "data: {{\"choices\":[{{\"delta\":{{\"tool_calls\":[{delta}]}}}}]}}\n\n"
            ))
        };
        let mut steps = Vec::new();
        for (index, id, name, arguments) in calls {
            let fragments = arguments
                .as_bytes()
                .chunks(16 * 1024)
                .map(|fragment| std::str::from_utf8(fragment).expect("ASCII arguments"));
            for (position, fragment) in fragments.enumerate() {
                steps.push(delta(if position == 0 {
                    call(*index, id, name, fragment)
                } else {
                    format!(
                        r#"{{"index":{index},"function":{{"arguments":{}}}}}"#,
                        serde_json::to_string(fragment).expect("JSON string")
                    )
                }));
            }
        }
        steps.push(chunk(
            "data: {\"choices\":[{\"delta\":{},\"finish_reason\":\"tool_calls\"}]}\n\n".into(),
        ));
        steps.push(usage_chunk((1, 1)));
        steps.push(Step::Chunk(DONE_CHUNK));
        steps
    }

    fn text_schema() -> Value {
        json!({
            "type": "object",
            "properties": {"text": {"type": "string"}},
            "required": ["text"],
            "additionalProperties": false
        })
    }

    /// Arguments for [`text_schema`] of exactly `bytes` bytes.
    fn text_arguments(prefix: &str, bytes: usize) -> String {
        let padding = bytes - r#"{"text":""}"#.len() - prefix.len();
        format!(r#"{{"text":"{prefix}{}"}}"#, "x".repeat(padding))
    }

    /// Registers and authorizes `echo`, which takes [`text_schema`].
    fn echo_registry(effect: ToolEffect, answer: Value) -> (ToolRegistry, Arc<AtomicUsize>) {
        let registry = ToolRegistry::default();
        let calls = Arc::new(AtomicUsize::new(0));
        registry
            .register(
                "echo",
                None,
                text_schema(),
                Arc::new(TestTool {
                    behavior: Behavior::Act(Box::new(move || answer.clone())),
                    calls: calls.clone(),
                    effect,
                }),
            )
            .expect("register");
        registry.authorize("echo").expect("authorize");
        (registry, calls)
    }

    #[test]
    fn tool_evidence_is_keyed_by_its_call_and_each_fact_is_written_once() {
        let (_root, directory) = data_directory("tool-evidence-keys");
        let worker = DatabaseWorker::start(&directory).expect("start database");
        let handle = worker.handle();
        let (_provider, llm) = llm_setup(&handle, "model-a");
        let run = observed_run(&handle, llm, 2);
        let other = observed_run(&handle, llm, 1);
        let job = run.job;
        let effect = ToolEffect::NonIdempotent;
        let decide = |evidence: NewToolEvidence| wait(handle.record_tool_decision(evidence));
        let conclude = |call: ToolCallRef, outcome: ToolOutcome| {
            wait(handle.record_tool_outcome(call, outcome))
        };

        // Calls of one request, of a later request and of another Job.
        for (call, id) in [
            (tool_call(job, 1, 0), "x"),
            (tool_call(job, 1, 1), "y"),
            (tool_call(job, 2, 0), "z"),
            (tool_call(other.job, 1, 0), "x"),
        ] {
            decide(admitted(call, id, effect)).expect("admit");
        }
        decide(refused(tool_call(job, 2, 1), "r")).expect("refuse");
        assert_eq!(
            evidence(&handle, other.job).len(),
            1,
            "another Job may use the same Provider id"
        );

        let before = evidence(&handle, job);
        for (result, expected) in [
            (
                decide(admitted(tool_call(job, 1, 0), "w", effect)),
                Violation::DuplicateToolCall(tool_call(job, 1, 0)),
            ),
            (
                decide(refused(tool_call(job, 1, 0), "w")),
                Violation::DuplicateToolCall(tool_call(job, 1, 0)),
            ),
            (
                decide(admitted(tool_call(job, 2, 2), "x", effect)),
                Violation::DuplicateToolCallId {
                    call: tool_call(job, 2, 2),
                    existing: tool_call(job, 1, 0),
                },
            ),
            (
                decide(admitted(tool_call(job, 3, 0), "q", effect)),
                Violation::RequestNotObserved {
                    job,
                    request: number(3),
                },
            ),
            (
                decide(admitted(tool_call(JobId::new(999_999), 1, 0), "q", effect)),
                Violation::NotFound(RecordRef::Job(JobId::new(999_999))),
            ),
            (
                conclude(tool_call(job, 3, 0), completed("{}")),
                Violation::ToolCallNotFound(tool_call(job, 3, 0)),
            ),
            (
                conclude(tool_call(job, 2, 1), completed("{}")),
                Violation::ToolCallNotAdmitted(tool_call(job, 2, 1)),
            ),
        ] {
            assert_eq!(violation(result), expected);
        }
        let not_started = llm_job(&handle, llm);
        assert_eq!(
            violation(decide(admitted(tool_call(not_started, 1, 0), "q", effect))),
            Violation::NotExecutingLocally(not_started),
            "only the Job's own execution records its calls"
        );

        // Malformed facts.
        type Change = Box<dyn Fn(&mut NewToolEvidence)>;
        let malformed: Vec<(&str, Change)> = vec![
            (
                "tool_call_id",
                Box::new(|evidence| evidence.tool_call_id.clear()),
            ),
            (
                "name",
                Box::new(|evidence| evidence.name = "no such name!".into()),
            ),
            (
                "handler_version",
                Box::new(|evidence| {
                    if let ToolDecision::Admitted(admission) = &mut evidence.decision {
                        admission.handler_version = "a version with spaces".into();
                    }
                }),
            ),
            (
                "arguments",
                Box::new(|evidence| {
                    if let ToolDecision::Admitted(admission) = &mut evidence.decision {
                        admission.arguments = ToolPayload::new("[1, 2]");
                    }
                }),
            ),
            (
                "parameters",
                Box::new(|evidence| {
                    if let ToolDecision::Admitted(admission) = &mut evidence.decision {
                        admission.parameters = "{".into();
                    }
                }),
            ),
        ];
        for (field, change) in malformed {
            let mut evidence = admitted(tool_call(job, 2, 3), "m", effect);
            change(&mut evidence);
            assert!(
                matches!(violation(decide(evidence)), Violation::InvalidValue(found, _) if found == field),
                "{field}"
            );
        }
        assert_eq!(
            evidence(&handle, job),
            before,
            "nothing refused was written"
        );

        // An outcome is written once, and never over an uncertain one.
        conclude(tool_call(job, 1, 0), completed(r#"{"sum":3}"#)).expect("complete");
        conclude(tool_call(job, 1, 1), uncertain()).expect("record the uncertainty");
        for call in [tool_call(job, 1, 0), tool_call(job, 1, 1)] {
            assert_eq!(
                violation(conclude(call, completed("{}"))),
                Violation::ToolOutcomeAlreadyRecorded(call)
            );
        }
        let records = evidence(&handle, job);
        assert_eq!(
            records
                .iter()
                .map(|record| (
                    record.call,
                    record.tool_call_id.as_str(),
                    outcome_of(record)
                ))
                .collect::<Vec<_>>(),
            [
                (tool_call(job, 1, 0), "x", Some(completed(r#"{"sum":3}"#))),
                (tool_call(job, 1, 1), "y", Some(uncertain())),
                (tool_call(job, 2, 0), "z", None),
                (tool_call(job, 2, 1), "r", None),
            ]
        );
        assert_eq!(
            records
                .iter()
                .map(ToolEvidenceRecord::is_unresolved)
                .collect::<Vec<_>>(),
            [false, true, true, false],
            "an admission without a definite outcome is unresolved; a refusal never is"
        );
        assert_eq!(
            records
                .iter()
                .map(ToolEvidenceRecord::is_applied)
                .collect::<Vec<_>>(),
            [true, false, false, false]
        );
        assert_eq!(
            records[3].decision.fact,
            ToolDecision::Refused(ToolCallRefusal::Unauthorized)
        );
        assert!(records.iter().all(|record| {
            record
                .outcome
                .as_ref()
                .is_none_or(|outcome| outcome.at >= record.decision.at)
        }));
        for after in [(number(1), 1), (number(1), u32::MAX)] {
            assert_eq!(
                wait(handle.tool_evidence(job, Some(after), 10)).expect("page"),
                records[2..],
                "the page after {after:?}"
            );
        }
        worker.shutdown().expect("shutdown");
        assert_relationships_consistent(&directory);
    }

    #[test]
    fn a_handler_runs_only_once_its_admission_is_durable_and_its_result_is_recorded_before_it_is_sent()
     {
        let (_root, directory) = data_directory("tool-evidence-write-ahead");
        let worker = DatabaseWorker::start(&directory).expect("start database");
        let handle = worker.handle();
        let (_provider, llm) = llm_setup(&handle, "model-a");
        let run = local_run(&handle, llm);
        let seen = Arc::new(std::sync::Mutex::new(Vec::new()));
        let observer = {
            let (handle, seen, job) = (handle.clone(), seen.clone(), run.job);
            Behavior::Act(Box::new(move || {
                seen.lock().expect("lock").push(evidence(&handle, job));
                json!({"sum": 5})
            }))
        };
        let (registry, calls) = registered_as(ToolEffect::NonIdempotent, vec![("add", observer)]);
        let looped = run_loop(
            &handle,
            run,
            Some(execution(&registry, &["add"], ToolLoopLimits::default())),
            vec![
                calling(&[add(0, "call_1", 2, 3)], (1, 1)),
                answering("The sum is 5.", (1, 1)),
            ],
            CancellationToken::default(),
            |_| {},
        );
        let sent = looped.sent.clone();
        looped.settled.expect("the loop settles");
        assert_eq!(calls.load(Ordering::SeqCst), 1);

        let seen = seen.lock().expect("lock");
        let [at_invocation] = seen.as_slice() else {
            panic!("the handler ran once");
        };
        let call = tool_call(run.job, 1, 0);
        let expected = ToolAdmission {
            handler_version: "test-1".into(),
            effect: ToolEffect::NonIdempotent,
            parameters: add_schema().to_string(),
            arguments: ToolPayload::new(r#"{"a":2,"b":3}"#),
            authorization: AuthorizationBasis::RegistryGrant,
        };
        match at_invocation.as_slice() {
            [record] => {
                assert_eq!(record.call, call);
                assert_eq!(
                    record.admission(),
                    Some(&expected),
                    "admitted before it ran"
                );
                assert_eq!(record.outcome, None, "and nothing of its outcome yet");
            }
            other => panic!("expected the one admission, found {other:?}"),
        }

        let records = evidence(&handle, run.job);
        let [record] = records.as_slice() else {
            panic!("one call, one record");
        };
        assert_eq!(record.tool_call_id, "call_1");
        assert_eq!(record.name, "add");
        assert_eq!(record.admission(), Some(&expected));
        assert_eq!(outcome_of(record), Some(completed(r#"{"sum":5}"#)));
        assert_eq!(
            answer_to(&sent[1], "call_1").as_deref(),
            Some(r#"{"sum":5}"#),
            "what was sent is what was recorded"
        );
        assert!(record.is_applied() && !record.is_unresolved());
        assert!(job_record(&handle, run.job).completed_at.is_some());
        worker.shutdown().expect("shutdown");
        assert_relationships_consistent(&directory);
    }

    #[test]
    fn an_admission_the_database_refuses_runs_nothing() {
        let (_root, directory) = data_directory("tool-evidence-admission-refused");
        let worker = DatabaseWorker::start(&directory).expect("start database");
        let handle = worker.handle();
        let (_provider, llm) = llm_setup(&handle, "model-a");
        let run = local_run(&handle, llm);
        let (registry, calls) =
            registered_as(ToolEffect::NonIdempotent, vec![("add", Behavior::Add)]);
        let writer = handle.clone();
        let looped = run_loop(
            &handle,
            run,
            Some(execution(&registry, &["add"], ToolLoopLimits::default())),
            vec![calling(&[add(0, "c", 1, 2)], (1, 1))],
            CancellationToken::default(),
            move |_| {
                // Another decision took the call's identity first.
                wait(writer.record_tool_decision(refused(tool_call(run.job, 1, 0), "elsewhere")))
                    .expect("take the call's identity");
            },
        );
        let (receipt, stop) = stopped(looped);
        assert_eq!(
            stop,
            ToolLoopStop::AdmissionRefused {
                index: 0,
                violation: Violation::DuplicateToolCall(tool_call(run.job, 1, 0)),
            }
        );
        assert_eq!(calls.load(Ordering::SeqCst), 0, "nothing ran");
        assert!(receipt.result.is_some());
        assert!(job_record(&handle, run.job).failed_at.is_some());
        let records = evidence(&handle, run.job);
        assert_eq!(
            records
                .iter()
                .map(|record| record.tool_call_id.as_str())
                .collect::<Vec<_>>(),
            ["elsewhere"]
        );
        worker.shutdown().expect("shutdown");
    }

    #[test]
    fn an_admission_storage_refuses_or_leaves_unknown_runs_nothing() {
        for unknown in [false, true] {
            let (_root, directory) = data_directory(if unknown {
                "tool-evidence-admission-unknown"
            } else {
                "tool-evidence-admission-unstored"
            });
            let worker = DatabaseWorker::start_with_config(&directory, lenient()).expect("start");
            let handle = worker.handle();
            let (_provider, llm) = llm_setup(&handle, "model-a");
            let run = local_run(&handle, llm);
            let (registry, calls) =
                registered_as(ToolEffect::NonIdempotent, vec![("add", Behavior::Add)]);
            let (seam_handle, path) = (handle.clone(), directory.database_path().to_path_buf());
            let failed_group = Arc::new(std::sync::Mutex::new(None));
            let group = failed_group.clone();
            dispatch_seam::install(move |point, call| {
                if point == Point::BeforeAdmission {
                    if unknown {
                        *group.lock().expect("lock") =
                            Some(fail_next_commit(&seam_handle, call.job));
                    } else {
                        protect(&seam_handle, &path);
                    }
                }
            });
            let looped = run_loop(
                &handle,
                run,
                Some(execution(&registry, &["add"], ToolLoopLimits::default())),
                vec![calling(&[add(0, "c", 1, 2)], (1, 1))],
                CancellationToken::default(),
                |_| {},
            );
            match looped.settled {
                Err(ExecutionRuntimeError::ToolLoopNotSettled {
                    stop,
                    evidence: Some(evidence),
                    ..
                }) => {
                    assert_eq!(
                        *stop,
                        ToolLoopStop::EvidenceNotRecorded {
                            index: 0,
                            outcome: None
                        }
                    );
                    assert_eq!(
                        matches!(*evidence, DatabaseError::OutcomeUnknown { .. }),
                        unknown,
                        "the database's own classification is kept: {evidence:?}"
                    );
                }
                other => panic!("the unrecorded admission must be reported, got {other:?}"),
            }
            assert_eq!(calls.load(Ordering::SeqCst), 0, "nothing ran");
            if unknown {
                let companion = failed_group
                    .lock()
                    .expect("lock")
                    .take()
                    .expect("the group was failed")
                    .join()
                    .expect("the companion finished");
                assert!(matches!(
                    companion,
                    Err(DatabaseError::OutcomeUnknown { .. })
                ));
                wait(handle.recover_storage()).expect("recover");
            }
            worker.shutdown().expect("shutdown");

            let worker = DatabaseWorker::start_with_config(&directory, lenient()).expect("restart");
            let handle = worker.handle();
            // Recovering the storage already reopened it and abandoned the claim.
            assert_eq!(
                handle.abandoned_at_open(),
                if unknown { Vec::new() } else { vec![run.job] }
            );
            let job = job_record(&handle, run.job);
            // The opening is gone, so the outcome is unknown. It admitted no
            // call that spawns a process, so nothing of it can still run and
            // the stop is confirmed at once.
            assert!(job.unknown_at.is_some() && job.stop_confirmed_at.is_some());
            assert!(blocking(&handle, llm).is_empty());
            let records = evidence(&handle, run.job);
            if unknown {
                assert!(
                    records.len() <= 1
                        && records
                            .iter()
                            .all(|record| record.outcome.is_none() && record.is_unresolved()),
                    "an admission that may have committed stays unresolved: {records:?}"
                );
            } else {
                assert!(records.is_empty(), "a refused write committed nothing");
            }
            worker.shutdown().expect("shutdown");
            assert_relationships_consistent(&directory);
        }
    }

    #[test]
    fn a_crash_after_the_admission_or_while_the_handler_runs_leaves_the_call_unresolved() {
        let cases = [
            (
                "after the admission",
                Behavior::Add,
                Some(Point::Admitted),
                0,
            ),
            (
                "after the handler ran, before its outcome",
                Behavior::Add,
                Some(Point::Ran),
                1,
            ),
        ];
        for (case, behavior, crash_at, ran) in cases {
            let (_root, directory) = data_directory(&format!("tool-evidence-crash-{ran}"));
            let worker = DatabaseWorker::start(&directory).expect("start database");
            let handle = worker.handle();
            let (_provider, llm) = llm_setup(&handle, "model-a");
            let run = local_run(&handle, llm);
            let (registry, calls) =
                registered_as(ToolEffect::NonIdempotent, vec![("add", behavior)]);
            let tools = execution(&registry, &["add"], ToolLoopLimits::default());
            let loop_handle = handle.clone();
            let died = std::thread::spawn(move || {
                if let Some(crash_at) = crash_at {
                    dispatch_seam::install(move |point, _| {
                        assert_ne!(point, crash_at, "the process died here");
                    });
                }
                run_loop(
                    &loop_handle,
                    run,
                    Some(tools),
                    vec![
                        calling(&[add(0, "c", 1, 2)], (1, 1)),
                        answering("3", (1, 1)),
                    ],
                    CancellationToken::default(),
                    |_| {},
                );
            })
            .join();
            assert!(died.is_err(), "{case}: the process died");
            assert_eq!(calls.load(Ordering::SeqCst), ran, "{case}");
            assert!(
                !has_outcome(&job_record(&handle, run.job)),
                "{case}: nothing settled the Job before the restart"
            );
            worker.shutdown().expect("shutdown");

            let worker = DatabaseWorker::start(&directory).expect("restart");
            let handle = worker.handle();
            assert_eq!(handle.abandoned_at_open(), [run.job], "{case}");
            let job = job_record(&handle, run.job);
            assert!(
                job.unknown_at.is_some() && job.stop_confirmed_at.is_some(),
                "{case}: unknown, and nothing of it can still run"
            );
            assert!(
                blocking(&handle, llm).is_empty(),
                "{case}: the Profile is released"
            );
            let records = evidence(&handle, run.job);
            let [record] = records.as_slice() else {
                panic!("{case}: one admission");
            };
            assert!(
                record.admission().is_some()
                    && record.outcome.is_none()
                    && record.is_unresolved()
                    && !record.is_applied(),
                "{case}: an admission without an outcome stays unresolved after the restart"
            );
            for acknowledge in [false, true] {
                assert_eq!(
                    violation(wait(handle.retry_job(retry_of(run.job, llm, acknowledge)))),
                    Violation::UnresolvedToolEffect(record.call),
                    "{case}: no acknowledgement reaches the unresolved effect"
                );
            }
            assert_eq!(
                calls.load(Ordering::SeqCst),
                ran,
                "{case}: nothing ran again"
            );
            worker.shutdown().expect("shutdown");
            assert_relationships_consistent(&directory);
        }
    }

    /// Whether any outcome, confirmed or unknown, was recorded for the Job.
    fn has_outcome(job: &JobRecord) -> bool {
        job.completed_at.is_some()
            || job.failed_at.is_some()
            || job.cancelled_at.is_some()
            || job.unknown_at.is_some()
    }

    #[test]
    fn a_completed_call_whose_outcome_cannot_be_recorded_stays_uncertain() {
        for unknown in [false, true] {
            let (_root, directory) = data_directory(if unknown {
                "tool-evidence-outcome-unknown"
            } else {
                "tool-evidence-outcome-unstored"
            });
            let worker = DatabaseWorker::start_with_config(&directory, lenient()).expect("start");
            let handle = worker.handle();
            let (_provider, llm) = llm_setup(&handle, "model-a");
            let run = local_run(&handle, llm);
            // The handler completes its effect, then storage fails before its
            // outcome can be recorded.
            let failed_group = Arc::new(std::sync::Mutex::new(None));
            let failing = {
                let (handle, path) = (handle.clone(), directory.database_path().to_path_buf());
                let (group, job) = (failed_group.clone(), run.job);
                Behavior::Act(Box::new(move || {
                    if unknown {
                        *group.lock().expect("lock") = Some(fail_next_commit(&handle, job));
                    } else {
                        protect(&handle, &path);
                    }
                    json!({"sum": 3})
                }))
            };
            let (registry, calls) =
                registered_as(ToolEffect::NonIdempotent, vec![("add", failing)]);
            let looped = run_loop(
                &handle,
                run,
                Some(execution(&registry, &["add"], ToolLoopLimits::default())),
                vec![
                    calling(&[add(0, "c", 1, 2)], (1, 1)),
                    answering("3", (1, 1)),
                ],
                CancellationToken::default(),
                |_| {},
            );
            assert_eq!(looped.sent.len(), 1, "an unrecorded result is never sent");
            match looped.settled {
                Err(ExecutionRuntimeError::ToolLoopNotSettled {
                    stop,
                    source,
                    evidence: Some(evidence),
                    ..
                }) => {
                    assert_eq!(
                        *stop,
                        ToolLoopStop::EvidenceNotRecorded {
                            index: 0,
                            outcome: Some(ToolOutcomeKind::Completed),
                        },
                        "what the handler did is kept in the report"
                    );
                    assert_eq!(
                        matches!(*evidence, DatabaseError::OutcomeUnknown { .. }),
                        unknown
                    );
                    assert!(!matches!(source, DatabaseError::Rejected(_)));
                }
                other => panic!("the unrecorded outcome must be reported, got {other:?}"),
            }
            assert_eq!(calls.load(Ordering::SeqCst), 1);
            if unknown {
                let companion = failed_group
                    .lock()
                    .expect("lock")
                    .take()
                    .expect("the group was failed")
                    .join()
                    .expect("the companion finished");
                assert!(matches!(
                    companion,
                    Err(DatabaseError::OutcomeUnknown { .. })
                ));
                wait(handle.recover_storage()).expect("recover");
            }
            let job = job_record(&handle, run.job);
            // Recovering the storage reopens it, which settles the claim of the
            // earlier opening as unknown; nothing else claimed an outcome.
            assert!(
                job.completed_at.is_none()
                    && job.failed_at.is_none()
                    && job.cancelled_at.is_none()
                    && job.unknown_at.is_some() == unknown,
                "neither Completed nor Failed was claimed"
            );
            worker.shutdown().expect("shutdown");

            let worker = DatabaseWorker::start_with_config(&directory, lenient()).expect("restart");
            let handle = worker.handle();
            let job = job_record(&handle, run.job);
            assert!(job.unknown_at.is_some() && job.stop_confirmed_at.is_some());
            let records = evidence(&handle, run.job);
            let [record] = records.as_slice() else {
                panic!("one admission");
            };
            assert!(record.admission().is_some());
            if unknown {
                // The outcome's commit may or may not have become durable.
                assert!(
                    matches!(
                        outcome_of(record),
                        None | Some(ToolOutcome::Completed { .. })
                    ),
                    "{record:?}"
                );
            } else {
                assert_eq!(record.outcome, None);
                assert!(record.is_unresolved(), "never taken as not applied");
            }
            assert_eq!(calls.load(Ordering::SeqCst), 1, "nothing ran again");
            worker.shutdown().expect("shutdown");
            assert_relationships_consistent(&directory);
        }
    }

    #[test]
    fn an_earlier_call_keeps_its_recorded_outcome_when_a_later_call_fails() {
        let (_root, directory) = data_directory("tool-evidence-later-failure");
        let worker = DatabaseWorker::start(&directory).expect("start database");
        let handle = worker.handle();
        let (_provider, llm) = llm_setup(&handle, "model-a");
        let cases = [
            (
                "fail",
                ToolLoopStop::HandlerFailed {
                    index: 1,
                    message: "the tool failed".into(),
                },
                ToolOutcomeKind::NotApplied,
            ),
            (
                "unsure",
                ToolLoopStop::HandlerUncertain {
                    index: 1,
                    message: "the connection dropped".into(),
                },
                ToolOutcomeKind::Uncertain,
            ),
            (
                "big",
                ToolLoopStop::ResultTooLarge { index: 1 },
                ToolOutcomeKind::CompletedWithheld,
            ),
        ];
        for (second, expected, kind) in cases {
            let run = local_run(&handle, llm);
            let (registry, calls) = registered_as(
                ToolEffect::NonIdempotent,
                vec![
                    ("add", Behavior::Add),
                    ("fail", Behavior::Fail),
                    ("unsure", Behavior::Uncertain),
                    ("big", Behavior::Big(20_000)),
                ],
            );
            let looped = run_loop(
                &handle,
                run,
                Some(execution(
                    &registry,
                    &["add", "fail", "unsure", "big"],
                    ToolLoopLimits::default(),
                )),
                vec![calling(
                    &[add(0, "a", 1, 2), call(1, "b", second, r#"{"a":1,"b":2}"#)],
                    (1, 1),
                )],
                CancellationToken::default(),
                |_| {},
            );
            assert_eq!(looped.sent.len(), 1, "{second}: nothing was sent back");
            let (receipt, stop) = stopped(looped);
            assert_eq!(stop, expected, "{second}");
            assert_eq!(calls.load(Ordering::SeqCst), 2, "{second}");
            let records = evidence(&handle, run.job);
            assert_eq!(
                records
                    .iter()
                    .map(|record| record.outcome.as_ref().map(|outcome| outcome.fact.kind()))
                    .collect::<Vec<_>>(),
                [Some(ToolOutcomeKind::Completed), Some(kind)],
                "{second}"
            );
            assert_eq!(outcome_of(&records[0]), Some(completed(r#"{"sum":3}"#)));
            if kind == ToolOutcomeKind::CompletedWithheld {
                assert_eq!(
                    outcome_of(&records[1]),
                    Some(ToolOutcome::CompletedWithheld {
                        reason: WithheldReason::TooLarge
                    }),
                    "the completed handler is recorded as such, without its result"
                );
            }
            let job = job_record(&handle, run.job);
            if kind == ToolOutcomeKind::Uncertain {
                assert!(job.unknown_at.is_some() && job.stop_confirmed_at.is_some());
                assert_eq!(receipt.result, None);
                assert_eq!(
                    violation(wait(handle.retry_job(retry_of(run.job, llm, true)))),
                    Violation::UnresolvedToolEffect(tool_call(run.job, 1, 1)),
                    "{second}"
                );
            } else {
                assert!(job.failed_at.is_some(), "{second}");
                assert_eq!(
                    violation(wait(handle.retry_job(retry_of(run.job, llm, false)))),
                    Violation::RepeatedEffectNotAcknowledged(tool_call(run.job, 1, 0)),
                    "{second}: the first call's effect happened"
                );
                wait(handle.retry_job(retry_of(run.job, llm, true)))
                    .expect("an acknowledged retry");
            }
            assert!(blocking(&handle, llm).iter().all(|held| *held != run.job));
        }
        worker.shutdown().expect("shutdown");
        assert_relationships_consistent(&directory);
    }

    #[test]
    fn a_grant_withdrawn_after_the_admission_refuses_the_invocation_and_is_recorded() {
        for unstored in [false, true] {
            let (_root, directory) = data_directory(if unstored {
                "tool-evidence-revoked-unstored"
            } else {
                "tool-evidence-revoked"
            });
            let worker = DatabaseWorker::start_with_config(&directory, lenient()).expect("start");
            let handle = worker.handle();
            let (_provider, llm) = llm_setup(&handle, "model-a");
            let run = local_run(&handle, llm);
            let (registry, calls) =
                registered_as(ToolEffect::NonIdempotent, vec![("add", Behavior::Add)]);
            let (revoker, seam_handle, path) = (
                registry.clone(),
                handle.clone(),
                directory.database_path().to_path_buf(),
            );
            dispatch_seam::install(move |point, _| {
                if point == Point::Admitted {
                    revoker.revoke("add").expect("revoke");
                    if unstored {
                        protect(&seam_handle, &path);
                    }
                }
            });
            let looped = run_loop(
                &handle,
                run,
                Some(execution(&registry, &["add"], ToolLoopLimits::default())),
                vec![calling(&[add(0, "c", 1, 2)], (1, 1))],
                CancellationToken::default(),
                |_| {},
            );
            assert_eq!(
                calls.load(Ordering::SeqCst),
                0,
                "the handler was never invoked"
            );
            if unstored {
                match looped.settled {
                    Err(ExecutionRuntimeError::ToolLoopNotSettled { stop, .. }) => assert_eq!(
                        *stop,
                        ToolLoopStop::EvidenceNotRecorded {
                            index: 0,
                            outcome: Some(ToolOutcomeKind::NotInvoked),
                        }
                    ),
                    other => panic!("expected an unsettled stop, got {other:?}"),
                }
                worker.shutdown().expect("shutdown");
                let worker =
                    DatabaseWorker::start_with_config(&directory, lenient()).expect("restart");
                let handle = worker.handle();
                let records = evidence(&handle, run.job);
                assert!(
                    records.len() == 1
                        && records[0].outcome.is_none()
                        && records[0].is_unresolved(),
                    "an unrecorded refusal keeps the call uncertain: {records:?}"
                );
                worker.shutdown().expect("shutdown");
            } else {
                let (_, stop) = stopped(looped);
                assert!(
                    matches!(&stop, ToolLoopStop::Refused(refusal)
                        if refusal.kind == ToolCallRefusal::Unauthorized && refusal.index == Some(0)),
                    "{stop:?}"
                );
                let records = evidence(&handle, run.job);
                let [record] = records.as_slice() else {
                    panic!("one admission");
                };
                assert!(record.admission().is_some());
                assert_eq!(
                    outcome_of(record),
                    Some(ToolOutcome::NotInvoked {
                        reason: NotInvokedReason::Refused(ToolCallRefusal::Unauthorized)
                    })
                );
                assert!(!record.is_unresolved() && !record.is_applied());
                assert!(job_record(&handle, run.job).failed_at.is_some());
                wait(handle.retry_job(retry_of(run.job, llm, false)))
                    .expect("a call never invoked repeats nothing");
                worker.shutdown().expect("shutdown");
            }
            assert_relationships_consistent(&directory);
        }
    }

    #[test]
    fn a_job_with_an_unresolved_tool_effect_can_only_become_unknown() {
        let (_root, directory) = data_directory("tool-evidence-settlement");
        let worker = DatabaseWorker::start(&directory).expect("start database");
        let handle = worker.handle();
        let (_provider, llm) = llm_setup(&handle, "model-a");

        for recorded in [None, Some(uncertain())] {
            for finalization in [
                JobFinalization::Completed(result("done")),
                JobFinalization::Failed(result("failed")),
                JobFinalization::Cancelled(result("cancelled")),
            ] {
                let run = observed_run(&handle, llm, 1);
                let call = tool_call(run.job, 1, 0);
                wait(handle.record_tool_decision(admitted(call, "c", ToolEffect::ReadOnly)))
                    .expect("admit");
                if let Some(outcome) = recorded.clone() {
                    wait(handle.record_tool_outcome(call, outcome)).expect("record");
                }
                wait(handle.finalize_job(run.job, finalization)).expect("finalize");
                let job = job_record(&handle, run.job);
                assert!(
                    job.unknown_at.is_some()
                        && job.completed_at.is_none()
                        && job.failed_at.is_none()
                        && job.cancelled_at.is_none(),
                    "{recorded:?}: the unresolved effect makes the outcome unknown"
                );
                assert!(
                    job.stop_confirmed_at.is_some() && !blocking(&handle, llm).contains(&run.job),
                    "nothing spawned, so the stop is confirmed and the Profile released"
                );
                let records = evidence(&handle, run.job);
                assert_eq!(
                    outcome_of(&records[0]),
                    recorded,
                    "finalizing changes no tool outcome"
                );
                assert!(
                    records[0].is_unresolved(),
                    "a confirmed stop is not a resolution"
                );
                assert_eq!(
                    violation(wait(handle.record_tool_outcome(
                        call,
                        ToolOutcome::NotApplied {
                            message: "after the fact".into()
                        }
                    ))),
                    Violation::NotExecutingLocally(run.job),
                    "nothing rewrites the history of a settled Job"
                );
            }
        }

        for outcome in [
            completed("{}"),
            ToolOutcome::CompletedWithheld {
                reason: WithheldReason::TooLarge,
            },
            ToolOutcome::NotApplied {
                message: "no effect".into(),
            },
            ToolOutcome::NotInvoked {
                reason: NotInvokedReason::Refused(ToolCallRefusal::Unauthorized),
            },
        ] {
            for finalization in [
                JobFinalization::Completed(result("done")),
                JobFinalization::Failed(result("failed")),
            ] {
                let run = observed_run(&handle, llm, 1);
                let call = tool_call(run.job, 1, 0);
                wait(handle.record_tool_decision(admitted(call, "c", ToolEffect::NonIdempotent)))
                    .expect("admit");
                wait(handle.record_tool_outcome(call, outcome.clone())).expect("record");
                wait(handle.finalize_job(run.job, finalization))
                    .expect("a definite outcome settles normally");
                assert!(!blocking(&handle, llm).contains(&run.job));
            }
        }
        worker.shutdown().expect("shutdown");
        let worker = DatabaseWorker::start(&directory).expect("restart");
        assert!(
            worker.handle().abandoned_at_open().is_empty(),
            "every local claim was released"
        );
        worker.shutdown().expect("shutdown");
        assert_relationships_consistent(&directory);
    }

    #[test]
    fn a_retry_never_repeats_an_unresolved_effect_and_repeats_a_known_one_only_when_acknowledged() {
        let (_root, directory) = data_directory("tool-evidence-retry");
        let worker = DatabaseWorker::start(&directory).expect("start database");
        let handle = worker.handle();
        let (_provider, llm) = llm_setup(&handle, "model-a");
        // A settled original whose one call, if any, had this effect and outcome.
        let original = |effect: Option<ToolEffect>, outcome: Option<ToolOutcome>| {
            let run = observed_run(&handle, llm, 1);
            let call = tool_call(run.job, 1, 0);
            if let Some(effect) = effect {
                wait(handle.record_tool_decision(admitted(call, "c", effect))).expect("admit");
                if let Some(outcome) = outcome.clone() {
                    wait(handle.record_tool_outcome(call, outcome)).expect("record");
                }
            }
            if effect.is_none() || outcome.as_ref().is_some_and(ToolOutcome::is_definite) {
                wait(handle.finalize_job(run.job, JobFinalization::Completed(result("done"))))
                    .expect("complete");
            } else {
                settle_unknown(&handle, run.job);
            }
            (run.job, call)
        };
        use ToolEffect::{Idempotent, NonIdempotent, ReadOnly};
        let not_applied = ToolOutcome::NotApplied {
            message: "no effect".into(),
        };
        let withheld = ToolOutcome::CompletedWithheld {
            reason: WithheldReason::TooLarge,
        };
        let not_invoked = ToolOutcome::NotInvoked {
            reason: NotInvokedReason::Refused(ToolCallRefusal::Unauthorized),
        };
        type Expected = Option<fn(ToolCallRef) -> Violation>;
        let unresolved: Expected = Some(Violation::UnresolvedToolEffect);
        let unacknowledged: Expected = Some(Violation::RepeatedEffectNotAcknowledged);
        /// A case, its original's call, if any, and its outcome, whether the
        /// retry acknowledges repeated effects, and its expected refusal.
        type Case = (
            &'static str,
            Option<ToolEffect>,
            Option<ToolOutcome>,
            bool,
            Expected,
        );
        let cases: Vec<Case> = vec![
            ("no tool effect", None, None, false, None),
            (
                "a read-only call",
                Some(ReadOnly),
                Some(completed("{}")),
                false,
                None,
            ),
            (
                "an uncertain read-only call",
                Some(ReadOnly),
                None,
                false,
                None,
            ),
            (
                "an idempotent call",
                Some(Idempotent),
                Some(completed("{}")),
                false,
                None,
            ),
            (
                "an unresolved idempotent call",
                Some(Idempotent),
                None,
                true,
                unresolved,
            ),
            (
                "an uncertain idempotent call",
                Some(Idempotent),
                Some(uncertain()),
                true,
                unresolved,
            ),
            (
                "a completed effect",
                Some(NonIdempotent),
                Some(completed("{}")),
                false,
                unacknowledged,
            ),
            (
                "an acknowledged effect",
                Some(NonIdempotent),
                Some(completed("{}")),
                true,
                None,
            ),
            (
                "a withheld result",
                Some(NonIdempotent),
                Some(withheld),
                false,
                unacknowledged,
            ),
            (
                "an effect not applied",
                Some(NonIdempotent),
                Some(not_applied),
                false,
                None,
            ),
            (
                "a call never invoked",
                Some(NonIdempotent),
                Some(not_invoked),
                false,
                None,
            ),
            (
                "an acknowledged unresolved effect",
                Some(NonIdempotent),
                None,
                true,
                unresolved,
            ),
            (
                "an acknowledged uncertain effect",
                Some(NonIdempotent),
                Some(uncertain()),
                true,
                unresolved,
            ),
        ];
        for (case, effect, outcome, acknowledge, expected) in cases {
            let (job, call) = original(effect, outcome);
            let retried = wait(handle.retry_job(retry_of(job, llm, acknowledge)));
            match expected {
                None => {
                    let retry = retried.unwrap_or_else(|error| panic!("{case}: {error}"));
                    assert_eq!(job_record(&handle, retry).retry_of, Some(job), "{case}");
                }
                Some(refusal) => {
                    assert_eq!(violation(retried), refusal(call), "{case}");
                    assert!(
                        wait(handle.retries(job)).expect("retries").is_empty(),
                        "{case}: no retry was created"
                    );
                }
            }
        }

        // A verified reconciliation resolves the effect as it found it.
        for (conclusion, expected) in [
            (EffectConclusion::Applied, unacknowledged),
            (EffectConclusion::NotApplied, None),
        ] {
            let (job, call) = original(Some(NonIdempotent), None);
            wait(handle.reconcile_tool_effect(call, verified(conclusion))).expect("reconcile");
            let retried = wait(handle.retry_job(retry_of(job, llm, false)));
            match expected {
                None => {
                    retried.expect("nothing to repeat");
                }
                Some(refusal) => {
                    assert_eq!(violation(retried), refusal(call));
                    wait(handle.retry_job(retry_of(job, llm, true)))
                        .expect("an acknowledged retry");
                }
            }
        }

        // Concurrent retries meet the same gate in their own transactions.
        let concurrently = |job: JobId, acknowledge: bool| {
            let attempts = [0, 1].map(|_| {
                let handle = handle.clone();
                std::thread::spawn(move || wait(handle.retry_job(retry_of(job, llm, acknowledge))))
            });
            attempts.map(|attempt| attempt.join().expect("the attempt finished"))
        };
        let (job, _) = original(Some(NonIdempotent), Some(completed("{}")));
        let [first, second] = concurrently(job, true);
        let (created, refused): (Vec<_>, Vec<_>) =
            [first, second].into_iter().partition(Result::is_ok);
        assert_eq!(created.len(), 1, "one acknowledged retry becomes active");
        assert!(matches!(
            violation(refused.into_iter().next().expect("the other")),
            Violation::ActiveRetry { original, .. } if original == job
        ));
        for (outcome, acknowledge, refusal) in [
            (
                Some(completed("{}")),
                false,
                Violation::RepeatedEffectNotAcknowledged as fn(_) -> _,
            ),
            (None, true, Violation::UnresolvedToolEffect),
        ] {
            let (job, call) = original(Some(NonIdempotent), outcome);
            for attempt in concurrently(job, acknowledge) {
                assert_eq!(violation(attempt), refusal(call));
            }
            assert!(wait(handle.retries(job)).expect("retries").is_empty());
        }
        worker.shutdown().expect("shutdown");
        assert_relationships_consistent(&directory);
    }

    #[test]
    fn a_reconciliation_is_verified_once_and_keeps_the_uncertainty_it_resolves() {
        let (_root, directory) = data_directory("tool-evidence-reconciliation");
        let worker = DatabaseWorker::start(&directory).expect("start database");
        let handle = worker.handle();
        let (_provider, llm) = llm_setup(&handle, "model-a");
        let run = observed_run(&handle, llm, 1);
        let job = run.job;
        let [uncertain_call, refused_call, resolved_call] =
            [0, 1, 2].map(|index| tool_call(job, 1, index));
        let decide = |evidence| wait(handle.record_tool_decision(evidence)).expect("decide");
        decide(admitted(uncertain_call, "u", ToolEffect::NonIdempotent));
        decide(refused(refused_call, "r"));
        decide(admitted(resolved_call, "d", ToolEffect::NonIdempotent));
        wait(handle.record_tool_outcome(uncertain_call, uncertain())).expect("record");
        wait(handle.record_tool_outcome(resolved_call, completed("{}"))).expect("record");
        let reconcile =
            |call, reconciliation| wait(handle.reconcile_tool_effect(call, reconciliation));

        assert_eq!(
            violation(reconcile(
                uncertain_call,
                verified(EffectConclusion::Applied)
            )),
            Violation::ToolEffectStillExecuting(job),
            "nothing is reconciled while the Job may still act"
        );
        settle_unknown(&handle, job);
        let before = evidence(&handle, job);
        for (call, expected) in [
            (
                tool_call(job, 1, 9),
                Violation::ToolCallNotFound(tool_call(job, 1, 9)),
            ),
            (refused_call, Violation::ToolCallNotAdmitted(refused_call)),
            (
                resolved_call,
                Violation::ToolEffectAlreadyResolved(resolved_call),
            ),
        ] {
            assert_eq!(
                violation(reconcile(call, verified(EffectConclusion::NotApplied))),
                expected
            );
        }
        for field in ["reconciler", "evidence", "explanation"] {
            let mut unverified = verified(EffectConclusion::NotApplied);
            match field {
                "reconciler" => unverified.reconciler.clear(),
                "evidence" => unverified.evidence.clear(),
                _ => unverified.explanation.clear(),
            }
            assert!(
                matches!(violation(reconcile(uncertain_call, unverified)), Violation::InvalidValue(found, _) if found == field),
                "{field}"
            );
        }
        assert_eq!(
            evidence(&handle, job),
            before,
            "nothing refused was written"
        );

        reconcile(uncertain_call, verified(EffectConclusion::Applied)).expect("reconcile");
        for again in [EffectConclusion::Applied, EffectConclusion::NotApplied] {
            assert_eq!(
                violation(reconcile(uncertain_call, verified(again))),
                Violation::ToolEffectAlreadyReconciled(uncertain_call),
                "{again:?}"
            );
        }
        let records = evidence(&handle, job);
        let record = &records[0];
        assert_eq!(record.decision, before[0].decision, "the admission is kept");
        assert_eq!(
            outcome_of(record),
            Some(uncertain()),
            "so is the uncertainty"
        );
        assert_eq!(
            record
                .reconciliation
                .as_ref()
                .map(|recorded| &recorded.fact),
            Some(&verified(EffectConclusion::Applied))
        );
        assert!(!record.is_unresolved() && record.is_applied());
        assert_eq!(&records[1..], &before[1..]);
        worker.shutdown().expect("shutdown");
        assert_relationships_consistent(&directory);
    }

    #[test]
    fn arguments_beyond_the_recorded_limits_are_refused_before_anything_runs() {
        let (_root, directory) = data_directory("tool-evidence-bounds");
        let worker = DatabaseWorker::start(&directory).expect("start database");
        let handle = worker.handle();
        let (_provider, llm) = llm_setup(&handle, "model-a");

        // The database refuses them as well.
        let run = observed_run(&handle, llm, 1);
        let call = |index| tool_call(run.job, 1, index);
        let at_limit = text_arguments("", MAX_STORED_TOOL_ARGUMENTS_BYTES);
        assert_eq!(
            violation(wait(handle.record_tool_decision(admitted_with(
                call(0),
                "c0",
                ToolEffect::ReadOnly,
                &text_arguments("", MAX_STORED_TOOL_ARGUMENTS_BYTES + 1),
            )))),
            Violation::ToolEvidenceLimit {
                call: call(0),
                limit: "the arguments of one call"
            }
        );
        let fitting = MAX_STORED_TOOL_ARGUMENTS_PER_JOB_BYTES / MAX_STORED_TOOL_ARGUMENTS_BYTES;
        for index in 0..fitting {
            wait(handle.record_tool_decision(admitted_with(
                call(index as u32),
                &format!("c{index}"),
                ToolEffect::ReadOnly,
                &at_limit,
            )))
            .expect("within the Job's limit");
        }
        assert_eq!(
            violation(wait(handle.record_tool_decision(admitted(
                call(fitting as u32),
                "over",
                ToolEffect::ReadOnly,
            )))),
            Violation::ToolEvidenceLimit {
                call: call(fitting as u32),
                limit: "the arguments of one Job"
            }
        );

        // Through the loop: one call over its limit.
        let run = local_run(&handle, llm);
        let (registry, calls) = echo_registry(ToolEffect::NonIdempotent, json!({"echoed": true}));
        let looped = run_loop(
            &handle,
            run,
            Some(execution(&registry, &["echo"], ToolLoopLimits::default())),
            vec![calling_in_fragments(&[(
                0,
                "big",
                "echo",
                text_arguments("", MAX_STORED_TOOL_ARGUMENTS_BYTES + 1),
            )])],
            CancellationToken::default(),
            |_| {},
        );
        assert_eq!(looped.sent.len(), 1);
        let (_, stop) = stopped(looped);
        assert!(
            matches!(&stop, ToolLoopStop::Refused(refusal)
                if refusal.kind == ToolCallRefusal::LimitExceeded && refusal.index == Some(0)),
            "{stop:?}"
        );
        assert_eq!(calls.load(Ordering::SeqCst), 0);
        let records = evidence(&handle, run.job);
        assert_eq!(
            records
                .iter()
                .map(|record| &record.decision.fact)
                .collect::<Vec<_>>(),
            [&ToolDecision::Refused(ToolCallRefusal::LimitExceeded)],
            "the refusal is recorded, its arguments are not"
        );
        assert!(job_record(&handle, run.job).failed_at.is_some());

        // Through the loop: calls within their own limit, over the Job's.
        let run = local_run(&handle, llm);
        let (registry, calls) = echo_registry(ToolEffect::NonIdempotent, json!({"echoed": true}));
        let sixty = || text_arguments("", 60 * 1024);
        let calls_r1: Vec<_> = (0..66)
            .map(|i| (i as u32, format!("r1_{i}"), "echo", sixty()))
            .collect();
        let calls_r2: Vec<_> = (0..3)
            .map(|i| (i as u32, format!("r2_{i}"), "echo", sixty()))
            .collect();
        let calls_r1_refs: Vec<_> = calls_r1
            .iter()
            .map(|(i, id, name, args)| (*i, id.as_str(), *name, args.clone()))
            .collect();
        let calls_r2_refs: Vec<_> = calls_r2
            .iter()
            .map(|(i, id, name, args)| (*i, id.as_str(), *name, args.clone()))
            .collect();
        let looped = run_loop(
            &handle,
            run,
            Some(execution(&registry, &["echo"], ToolLoopLimits::default())),
            vec![
                calling_in_fragments(&calls_r1_refs),
                calling_in_fragments(&calls_r2_refs),
            ],
            CancellationToken::default(),
            |_| {},
        );
        let (_, stop) = stopped(looped);
        assert!(
            matches!(&stop, ToolLoopStop::Refused(refusal)
                if refusal.kind == ToolCallRefusal::LimitExceeded && refusal.index == Some(2)),
            "{stop:?}"
        );
        assert_eq!(
            calls.load(Ordering::SeqCst),
            66,
            "no call of the response over the limit ran"
        );
        let records = evidence(&handle, run.job);
        assert_eq!(records.len(), 67);
        assert_eq!(
            records.last().unwrap().decision.fact,
            ToolDecision::Refused(ToolCallRefusal::LimitExceeded)
        );
        assert_eq!(records.last().unwrap().call, tool_call(run.job, 2, 2));
        worker.shutdown().expect("shutdown");
        assert_relationships_consistent(&directory);
    }

    #[test]
    fn recorded_arguments_and_results_stay_out_of_refusals_and_debug_output() {
        const MARKER: &str = "s3cr3t-7f1c";
        let (_root, directory) = data_directory("tool-evidence-confidential");
        let worker = DatabaseWorker::start(&directory).expect("start database");
        let handle = worker.handle();
        let (_provider, llm) = llm_setup(&handle, "model-a");
        let leaks = |text: String| text.contains(MARKER);

        // A call that ran: its arguments and result are recorded, and shown
        // nowhere but in the record's own fields.
        let run = local_run(&handle, llm);
        let (registry, _) = echo_registry(ToolEffect::ReadOnly, json!({"echo": MARKER}));
        let looped = run_loop(
            &handle,
            run,
            Some(execution(&registry, &["echo"], ToolLoopLimits::default())),
            vec![
                calling(
                    &[call(0, "c", "echo", &format!(r#"{{"text":"{MARKER}"}}"#))],
                    (1, 1),
                ),
                answering("done", (1, 1)),
            ],
            CancellationToken::default(),
            |_| {},
        );
        looped.settled.expect("settles");
        let records = evidence(&handle, run.job);
        let admission = records[0].admission().expect("admitted");
        assert!(leaks(admission.arguments.as_str().to_owned()));
        assert!(matches!(
            outcome_of(&records[0]),
            Some(ToolOutcome::Completed { result }) if leaks(result.as_str().to_owned())
        ));
        assert!(!leaks(format!("{records:?}")), "Debug shows only sizes");

        // A call refused for its arguments.
        let run = local_run(&handle, llm);
        let (registry, calls) = echo_registry(ToolEffect::ReadOnly, json!({}));
        let looped = run_loop(
            &handle,
            run,
            Some(execution(&registry, &["echo"], ToolLoopLimits::default())),
            vec![calling_in_fragments(&[(
                0,
                "c",
                "echo",
                text_arguments(MARKER, MAX_STORED_TOOL_ARGUMENTS_BYTES + 1),
            )])],
            CancellationToken::default(),
            |_| {},
        );
        assert_eq!(calls.load(Ordering::SeqCst), 0);
        let error = looped.settled.expect_err("refused");
        assert!(!leaks(format!("{error} {error:?}")));
        let outputs = wait(handle.job_outputs(run.job, None, 100)).expect("outputs");
        assert!(!leaks(format!("{outputs:?}")), "nor does the Job's result");
        assert!(!leaks(format!("{:?}", evidence(&handle, run.job))));

        // The database's own refusal of such arguments.
        let run = observed_run(&handle, llm, 1);
        let refused = wait(handle.record_tool_decision(admitted_with(
            tool_call(run.job, 1, 0),
            "c",
            ToolEffect::ReadOnly,
            &text_arguments(MARKER, MAX_STORED_TOOL_ARGUMENTS_BYTES + 1),
        )))
        .expect_err("refused");
        assert!(!leaks(format!("{refused} {refused:?}")));
        worker.shutdown().expect("shutdown");
    }

    /// A handler that only declares a version.
    struct Versioned(&'static str);

    impl ToolHandler for Versioned {
        fn version(&self) -> &str {
            self.0
        }

        fn effect(&self) -> ToolEffect {
            ToolEffect::ReadOnly
        }

        fn call(&self, _arguments: Value) -> ToolFuture<'_> {
            Box::pin(async { Ok(json!({})) })
        }
    }

    #[test]
    fn a_registration_declares_a_stable_handler_version_and_a_recordable_schema() {
        use crate::orchestration::tools::ToolRegistryError;

        let registry = ToolRegistry::default();
        assert!(
            registry
                .offer(&[])
                .expect("nothing")
                .wires()
                .expect("none")
                .is_empty(),
            "no tool is registered by default"
        );
        for version in ["", "has space", &"v".repeat(65)] {
            let version: &'static str = Box::leak(version.to_owned().into_boxed_str());
            assert_eq!(
                registry
                    .register("tool", None, add_schema(), Arc::new(Versioned(version)))
                    .map(|_| ()),
                Err(ToolRegistryError::InvalidVersion("tool".into())),
                "{version:?}"
            );
        }
        let huge = json!({
            "type": "object",
            "description": "x".repeat(crate::openai_compatible::MAX_TOOL_DEFINITION_BYTES),
        });
        assert!(matches!(
            registry.register("tool", None, huge, Arc::new(Versioned("1.0.0"))),
            Err(ToolRegistryError::UnsupportedSchema(..))
        ));
        registry
            .register(
                "tool",
                None,
                add_schema(),
                Arc::new(Versioned("1.0.0+build-7")),
            )
            .expect("a valid version");
        assert_eq!(
            registry.offer(&["tool"]).map(|_| ()),
            Err(ToolRegistryError::Unauthorized("tool".into())),
            "registering still grants nothing"
        );
    }

    /// A settled Job whose one call, if any, had this effect and outcome.
    fn settled_original(
        handle: &DatabaseHandle,
        llm: ProfileId,
        effect: Option<ToolEffect>,
        outcome: Option<ToolOutcome>,
    ) -> (JobId, ToolCallRef) {
        let run = observed_run(handle, llm, 1);
        let call = tool_call(run.job, 1, 0);
        if let Some(effect) = effect {
            wait(handle.record_tool_decision(admitted(call, "c", effect))).expect("admit");
            if let Some(outcome) = outcome.clone() {
                wait(handle.record_tool_outcome(call, outcome)).expect("record");
            }
        }
        if effect.is_none() || outcome.as_ref().is_some_and(ToolOutcome::is_definite) {
            wait(handle.finalize_job(run.job, JobFinalization::Completed(result("done"))))
                .expect("complete");
        } else {
            settle_unknown(handle, run.job);
        }
        (run.job, call)
    }

    /// A retry of `original` that ran no tool and failed, so it is settled.
    fn failed_retry(
        handle: &DatabaseHandle,
        llm: ProfileId,
        original: JobId,
        acknowledge: bool,
    ) -> JobId {
        let retry = wait(handle.retry_job(retry_of(original, llm, acknowledge))).expect("retry");
        wait(handle.claim_fixture(retry, test_executor(handle))).expect("start the retry");
        wait(handle.finalize_job(retry, JobFinalization::Failed(result("no tool ran"))))
            .expect("fail the retry");
        retry
    }

    /// A retry of `original`, which must be refused for `expected` and leave
    /// nothing behind: no Job and no `retry` row.
    fn assert_retry_refused(
        handle: &DatabaseHandle,
        llm: ProfileId,
        original: JobId,
        acknowledge: bool,
        expected: Violation,
    ) {
        let jobs = wait(handle.jobs(None, 1_000)).expect("jobs").len();
        let retries = wait(handle.retries(original)).expect("retries");
        assert_eq!(
            violation(wait(handle.retry_job(retry_of(original, llm, acknowledge)))),
            expected
        );
        assert_eq!(wait(handle.jobs(None, 1_000)).expect("jobs").len(), jobs);
        assert_eq!(wait(handle.retries(original)).expect("retries"), retries);
    }

    #[test]
    fn a_retry_answers_for_the_tool_effects_of_its_whole_retry_lineage() {
        let (_root, directory) = data_directory("tool-evidence-retry-lineage");
        let worker = DatabaseWorker::start(&directory).expect("start database");
        let handle = worker.handle();
        let (_provider, llm) = llm_setup(&handle, "model-a");
        use ToolEffect::{Idempotent, NonIdempotent, ReadOnly};

        // An acknowledgement covers only the retry that gave it, however far
        // down the chain the effect happened.
        let (first, effect) =
            settled_original(&handle, llm, Some(NonIdempotent), Some(completed("{}")));
        let second = failed_retry(&handle, llm, first, true);
        assert_retry_refused(
            &handle,
            llm,
            second,
            false,
            Violation::RepeatedEffectNotAcknowledged(effect),
        );
        let third = failed_retry(&handle, llm, second, true);
        assert_retry_refused(
            &handle,
            llm,
            third,
            false,
            Violation::RepeatedEffectNotAcknowledged(effect),
        );
        let fourth =
            wait(handle.retry_job(retry_of(third, llm, true))).expect("an acknowledged retry");
        assert_eq!(job_record(&handle, fourth).retry_of, Some(third));

        // An unresolved effect is refused whatever the retry acknowledges.
        let (original, unresolved) = settled_original(&handle, llm, Some(NonIdempotent), None);
        for acknowledge in [false, true] {
            assert_retry_refused(
                &handle,
                llm,
                original,
                acknowledge,
                Violation::UnresolvedToolEffect(unresolved),
            );
        }

        // Effects that can be repeated, or that did not happen, ask for no
        // acknowledgement anywhere in the chain; a reconciliation counts as
        // what it found.
        let reconciled = |conclusion| {
            let (job, call) = settled_original(&handle, llm, Some(NonIdempotent), None);
            wait(handle.reconcile_tool_effect(call, verified(conclusion))).expect("reconcile");
            (job, call)
        };
        let unacknowledged = [
            settled_original(&handle, llm, Some(Idempotent), Some(completed("{}"))).0,
            settled_original(
                &handle,
                llm,
                Some(NonIdempotent),
                Some(ToolOutcome::NotApplied {
                    message: "no effect".into(),
                }),
            )
            .0,
            settled_original(
                &handle,
                llm,
                Some(NonIdempotent),
                Some(ToolOutcome::NotInvoked {
                    reason: NotInvokedReason::Refused(ToolCallRefusal::Unauthorized),
                }),
            )
            .0,
            settled_original(
                &handle,
                llm,
                Some(NonIdempotent),
                Some(ToolOutcome::NotInvoked {
                    reason: NotInvokedReason::Cancelled,
                }),
            )
            .0,
            reconciled(EffectConclusion::NotApplied).0,
            settled_original(&handle, llm, Some(ReadOnly), None).0,
        ];
        for original in unacknowledged {
            let middle = failed_retry(&handle, llm, original, false);
            let retry = wait(handle.retry_job(retry_of(middle, llm, false)))
                .unwrap_or_else(|error| panic!("a retry of {original}'s chain: {error}"));
            assert_eq!(job_record(&handle, retry).retry_of, Some(middle));
        }
        let (original, applied) = reconciled(EffectConclusion::Applied);
        let middle = failed_retry(&handle, llm, original, true);
        assert_retry_refused(
            &handle,
            llm,
            middle,
            false,
            Violation::RepeatedEffectNotAcknowledged(applied),
        );
        wait(handle.retry_job(retry_of(middle, llm, true))).expect("an acknowledged retry");

        // Concurrent retries of a chain meet the same gate, and at most one
        // becomes active.
        let (original, effect) =
            settled_original(&handle, llm, Some(NonIdempotent), Some(completed("{}")));
        let middle = failed_retry(&handle, llm, original, true);
        let concurrently = |acknowledge: bool| {
            [0, 1]
                .map(|_| {
                    let handle = handle.clone();
                    std::thread::spawn(move || {
                        wait(handle.retry_job(retry_of(middle, llm, acknowledge)))
                    })
                })
                .map(|attempt| attempt.join().expect("the attempt finished"))
        };
        for attempt in concurrently(false) {
            assert_eq!(
                violation(attempt),
                Violation::RepeatedEffectNotAcknowledged(effect)
            );
        }
        assert!(wait(handle.retries(middle)).expect("retries").is_empty());
        let (created, refused): (Vec<_>, Vec<_>) =
            concurrently(true).into_iter().partition(Result::is_ok);
        assert_eq!(created.len(), 1, "one acknowledged retry becomes active");
        assert!(matches!(
            violation(refused.into_iter().next().expect("the other")),
            Violation::ActiveRetry { original, .. } if original == middle
        ));
        worker.shutdown().expect("shutdown");
        assert_relationships_consistent(&directory);
    }

    #[test]
    fn a_cancellation_before_the_handler_is_invoked_keeps_it_from_running() {
        for unstored in [false, true] {
            let (_root, directory) = data_directory(if unstored {
                "tool-evidence-cancelled-unstored"
            } else {
                "tool-evidence-cancelled-admission"
            });
            let worker = DatabaseWorker::start_with_config(&directory, lenient()).expect("start");
            let handle = worker.handle();
            let (_provider, llm) = llm_setup(&handle, "model-a");
            let run = local_run(&handle, llm);
            let (registry, calls) =
                registered_as(ToolEffect::NonIdempotent, vec![("add", Behavior::Add)]);
            let cancellation = CancellationToken::default();
            let (canceller, seam_handle, path) = (
                cancellation.clone(),
                handle.clone(),
                directory.database_path().to_path_buf(),
            );
            let pending = Arc::new(std::sync::Mutex::new(None));
            let held = pending.clone();
            dispatch_seam::install(move |point, _| match point {
                // The cancellation arrives while the admission's write is
                // held in the database, which then commits it.
                Point::BeforeAdmission if !unstored => {
                    let (entered, release) = seam_handle.group_probe().hold_next_group();
                    let canceller = canceller.clone();
                    *held.lock().expect("lock") = Some(std::thread::spawn(move || {
                        entered
                            .recv_timeout(crate::test_support::WAIT)
                            .expect("the admission is held");
                        canceller.cancel();
                        release.send(()).expect("release the admission");
                    }));
                }
                Point::Admitted if unstored => {
                    canceller.cancel();
                    protect(&seam_handle, &path);
                }
                _ => {}
            });
            let looped = run_loop(
                &handle,
                run,
                Some(execution(&registry, &["add"], ToolLoopLimits::default())),
                vec![calling(&[add(0, "c", 1, 2)], (1, 1))],
                cancellation,
                |_| {},
            );
            assert_eq!(
                calls.load(Ordering::SeqCst),
                0,
                "the handler was never invoked"
            );
            let call = tool_call(run.job, 1, 0);
            if unstored {
                match looped.settled {
                    Err(ExecutionRuntimeError::ToolLoopNotSettled {
                        stop,
                        evidence: Some(evidence),
                        ..
                    }) => {
                        assert_eq!(*stop, ToolLoopStop::Cancelled);
                        assert!(
                            !matches!(*evidence, DatabaseError::Rejected(_)),
                            "the database's own error is kept: {evidence:?}"
                        );
                    }
                    other => panic!("expected an unsettled cancellation, got {other:?}"),
                }
                worker.shutdown().expect("shutdown");
                let worker =
                    DatabaseWorker::start_with_config(&directory, lenient()).expect("restart");
                let handle = worker.handle();
                let job = job_record(&handle, run.job);
                assert!(job.unknown_at.is_some() && job.stop_confirmed_at.is_some());
                let records = evidence(&handle, run.job);
                assert!(
                    records.len() == 1
                        && records[0].outcome.is_none()
                        && records[0].is_unresolved(),
                    "an unrecorded NotInvoked is not claimed: {records:?}"
                );
                assert_eq!(
                    records[0].call, call,
                    "the same admitted call is still the unresolved one"
                );
                assert_eq!(
                    violation(wait(handle.retry_job(retry_of(run.job, llm, true)))),
                    Violation::UnresolvedToolEffect(records[0].call)
                );
                worker.shutdown().expect("shutdown");
            } else {
                pending
                    .lock()
                    .expect("lock")
                    .take()
                    .expect("the admission was held")
                    .join()
                    .expect("the cancellation was sent");
                let (receipt, stop) = stopped(looped);
                assert_eq!(stop, ToolLoopStop::Cancelled);
                assert_eq!(receipt.result, None);
                let job = job_record(&handle, run.job);
                assert!(job.unknown_at.is_some() && job.stop_confirmed_at.is_some());
                assert!(blocking(&handle, llm).is_empty());
                let records = evidence(&handle, run.job);
                let [record] = records.as_slice() else {
                    panic!("one admission");
                };
                assert!(record.admission().is_some(), "the admission committed");
                assert_eq!(
                    outcome_of(record),
                    Some(ToolOutcome::NotInvoked {
                        reason: NotInvokedReason::Cancelled
                    }),
                    "a definite outcome: the handler never ran"
                );
                assert!(!record.is_unresolved() && !record.is_applied());
                wait(handle.retry_job(retry_of(run.job, llm, false)))
                    .expect("nothing to repeat and nothing unresolved");
                worker.shutdown().expect("shutdown");
            }
            assert_relationships_consistent(&directory);
        }
    }

    // ---- workspace_read_file ------------------------------------------------

    #[cfg(unix)]
    mod workspace_read_file {
        use super::*;
        use crate::orchestration::workspace_read::{self, Workspace};

        /// A Workspace holding `files`, beside the test's data directory.
        fn workspace_with(name: &str, files: &[(&str, &str)]) -> (TestRoot, std::path::PathBuf) {
            let (guard, root) = crate::test_support::temporary_directory(SUITE, name);
            for (path, content) in files {
                std::fs::write(root.join(path), content).expect("write a Workspace file");
            }
            (guard, root)
        }

        fn read_call(index: u32, id: &str, arguments: &str) -> String {
            call(index, id, workspace_read::TOOL_NAME, arguments)
        }

        /// The text of every tool result a conversation sends.
        fn tool_results(conversation: &ChatConversation) -> Vec<(String, String)> {
            conversation
                .messages
                .iter()
                .filter_map(|message| match (&message.tool_call_id, &message.content) {
                    (Some(id), Some(MessageContentWire::Text(text))) => {
                        Some((id.clone(), text.clone()))
                    }
                    _ => None,
                })
                .collect()
        }

        #[test]
        fn workspace_read_file_reads_the_authorized_file_for_the_final_answer() {
            let text = "# Demo\n\nA \"README\" with ünïcode, a tab\tand a backslash \\.\n";
            let (_workspace_guard, root) =
                workspace_with("workspace-read-files", &[("README.md", text)]);
            let (_root, directory) = data_directory("workspace-read");
            let worker = DatabaseWorker::start(&directory).expect("start database");
            let handle = worker.handle();
            let (_provider, llm) = llm_setup(&handle, "model-a");
            let run = local_run(&handle, llm);
            let tools = workspace_read::read_file_tools(
                Workspace::authorize(&root, &directory).expect("authorize the Workspace"),
            )
            .expect("enable the tool");

            let admitted_first = Arc::new(AtomicUsize::new(0));
            let (seen, seam_handle) = (admitted_first.clone(), handle.clone());
            dispatch_seam::install(move |point, call| {
                let records = wait(seam_handle.tool_evidence(call.job, None, 10)).expect("read");
                let [record] = records.as_slice() else {
                    if point == Point::BeforeAdmission {
                        return;
                    }
                    panic!("one call is admitted: {records:?}");
                };
                match point {
                    Point::BeforeAdmission => panic!("nothing is recorded before the admission"),
                    Point::Admitted => {
                        assert!(record.admission().is_some() && record.outcome.is_none());
                        seen.fetch_add(1, Ordering::SeqCst);
                    }
                    Point::Ran => {
                        assert_eq!(seen.load(Ordering::SeqCst), 1, "admitted before it ran");
                        assert!(record.outcome.is_none(), "no outcome before it is recorded");
                    }
                }
            });
            let looped = run_loop(
                &handle,
                run,
                Some(tools),
                vec![
                    calling(
                        &[read_call(0, "call_read", r#"{"path":"README.md"}"#)],
                        (11, 4),
                    ),
                    answering("The README introduces a demo.", (40, 9)),
                ],
                CancellationToken::default(),
                |_| {},
            );
            let receipt = looped.settled.expect("the loop settles");
            assert_eq!(receipt.job, run.job);
            assert_eq!(admitted_first.load(Ordering::SeqCst), 1);

            let [first, second] = looped.sent.as_slice() else {
                panic!("two Provider requests");
            };
            assert_eq!(
                first
                    .tools
                    .iter()
                    .map(|tool| tool.function.name.as_str())
                    .collect::<Vec<_>>(),
                [workspace_read::TOOL_NAME],
                "only the Workspace tool is offered"
            );
            let result = serde_json::to_string(&json!({"path": "README.md", "content": text}))
                .expect("JSON");
            assert_eq!(
                tool_results(second),
                [("call_read".to_owned(), result.clone())],
                "the next request carries the exact, complete file"
            );

            let records = evidence(&handle, run.job);
            let [record] = records.as_slice() else {
                panic!("one call: {records:?}");
            };
            assert_eq!(record.call, tool_call(run.job, 1, 0));
            assert_eq!(
                (record.tool_call_id.as_str(), record.name.as_str()),
                ("call_read", workspace_read::TOOL_NAME)
            );
            let admission = record.admission().expect("admitted");
            assert_eq!(admission.handler_version, "workspace_read_file-2");
            assert_eq!(admission.effect, ToolEffect::ReadOnly);
            assert_eq!(admission.arguments.as_str(), r#"{"path":"README.md"}"#);
            assert_eq!(admission.authorization, AuthorizationBasis::RegistryGrant);
            assert_eq!(outcome_of(record), Some(completed(&result)));

            let job = job_record(&handle, run.job);
            assert!(job.completed_at.is_some());
            let outputs = wait(handle.job_outputs(run.job, None, 100)).expect("outputs");
            let final_output = outputs.last().expect("a result");
            assert_eq!(final_output.kind, OutputKind::Result);
            assert_eq!(
                final_output.summary.as_deref(),
                Some("The README introduces a demo.")
            );
            assert!(blocking(&handle, llm).is_empty(), "the Profile is released");

            let requests = by_request(&handle, run.job);
            assert_eq!(requests.keys().copied().collect::<Vec<_>>(), [1, 2]);
            for (request, finish) in [
                (1, ChatFinishReason::ToolCalls),
                (2, ChatFinishReason::Stop),
            ] {
                let (_, stats) = &requests[&request];
                assert_eq!(
                    stats
                        .iter()
                        .map(|stat| stat.observation.finish_reason)
                        .collect::<Vec<_>>(),
                    [Some(finish)],
                    "request {request} has its own AvailabilityStat"
                );
            }
            let samples = TokenRequestSample::per_request(&job_metrics(&handle, run.job))
                .expect("per request");
            assert_eq!(
                samples
                    .iter()
                    .map(|sample| (sample.request, sample.total))
                    .collect::<Vec<_>>(),
                [(number(1), Some(15)), (number(2), Some(49))],
                "each request keeps its own usage"
            );

            // The evidence is what was read then; nothing reads the file again.
            std::fs::write(root.join("README.md"), "changed").expect("change the file");
            worker.shutdown().expect("shutdown");
            let worker = DatabaseWorker::start(&directory).expect("restart");
            let handle = worker.handle();
            assert!(handle.abandoned_at_open().is_empty());
            assert_eq!(evidence(&handle, run.job), records);
            assert!(job_record(&handle, run.job).completed_at.is_some());
            worker.shutdown().expect("shutdown");
            assert_relationships_consistent(&directory);
        }

        #[test]
        fn a_failed_workspace_read_sends_no_result_and_fails_the_job() {
            let (_outside_guard, outside) =
                workspace_with("workspace-read-outside", &[("secret.txt", "outside")]);
            let (_workspace_guard, root) =
                workspace_with("workspace-read-failures-files", &[("README.md", "inside")]);
            std::os::unix::fs::symlink(outside.join("secret.txt"), root.join("linked.txt"))
                .expect("link");
            std::fs::write(
                root.join("big.txt"),
                "a".repeat(workspace_read::MAX_FILE_BYTES + 1),
            )
            .expect("write");
            let (_root, directory) = data_directory("workspace-read-failures");
            let worker = DatabaseWorker::start(&directory).expect("start database");
            let handle = worker.handle();
            let (_provider, llm) = llm_setup(&handle, "model-a");
            for (path, message) in [
                ("missing.md", "no such file in the Workspace"),
                (
                    "../workspace-read-outside/secret.txt",
                    "the path is not a plain relative path inside the Workspace",
                ),
                ("linked.txt", "the path traverses a symbolic link"),
                ("big.txt", "the file is larger than 8192 bytes"),
            ] {
                let run = local_run(&handle, llm);
                let tools = workspace_read::read_file_tools(
                    Workspace::authorize(&root, &directory).expect("authorize"),
                )
                .expect("enable the tool");
                let arguments = json!({ "path": path }).to_string();
                let looped = run_loop(
                    &handle,
                    run,
                    Some(tools),
                    vec![
                        calling(&[read_call(0, "call_read", &arguments)], (1, 1)),
                        answering("never sent", (1, 1)),
                    ],
                    CancellationToken::default(),
                    |_| {},
                );
                assert_eq!(looped.sent.len(), 1, "{path}: no request carries a result");
                let (receipt, stop) = stopped(looped);
                assert_eq!(
                    stop,
                    ToolLoopStop::HandlerFailed {
                        index: 0,
                        message: message.to_owned()
                    },
                    "{path}"
                );
                assert!(
                    !message.contains(path),
                    "the refusal does not repeat the path"
                );
                assert!(receipt.result.is_some());
                let records = evidence(&handle, run.job);
                let [record] = records.as_slice() else {
                    panic!("{path}: one call");
                };
                assert_eq!(
                    outcome_of(record),
                    Some(ToolOutcome::NotApplied {
                        message: message.to_owned()
                    }),
                    "{path}: no completed result is recorded"
                );
                let job = job_record(&handle, run.job);
                assert!(job.failed_at.is_some(), "{path}");
                assert!(blocking(&handle, llm).is_empty(), "{path}");
            }
            worker.shutdown().expect("shutdown");
        }

        #[test]
        fn the_provider_cannot_choose_the_workspace_or_run_the_tool_unauthorized() {
            /// Where a call is refused: by the response reader, against the
            /// request's own tools, or at dispatch, against the registry.
            #[derive(Debug, PartialEq)]
            enum Refused {
                Stream,
                Dispatch(ToolCallRefusal),
            }
            let (_workspace_guard, root) =
                workspace_with("workspace-read-refusals-files", &[("README.md", "inside")]);
            let (_root, directory) = data_directory("workspace-read-refusals");
            let worker = DatabaseWorker::start(&directory).expect("start database");
            let handle = worker.handle();
            let (_provider, llm) = llm_setup(&handle, "model-a");
            let cases = [
                (
                    "a Workspace root of its own",
                    r#"{"path":"README.md","root":"/"}"#,
                    true,
                    false,
                    Refused::Stream,
                ),
                (
                    "no path",
                    r#"{"root":"/etc"}"#,
                    true,
                    false,
                    Refused::Stream,
                ),
                (
                    "a tool OCG did not offer",
                    r#"{"path":"README.md"}"#,
                    false,
                    false,
                    Refused::Stream,
                ),
                (
                    "a grant withdrawn after the offer",
                    r#"{"path":"README.md"}"#,
                    true,
                    true,
                    Refused::Dispatch(ToolCallRefusal::Unauthorized),
                ),
            ];
            for (case, arguments, offered, revoked, refused) in cases {
                let run = local_run(&handle, llm);
                let registry = ToolRegistry::default();
                workspace_read::register(
                    &registry,
                    Workspace::authorize(&root, &directory).expect("authorize"),
                )
                .expect("register");
                registry
                    .authorize(workspace_read::TOOL_NAME)
                    .expect("authorize the tool");
                let tools = offered.then(|| {
                    execution(
                        &registry,
                        &[workspace_read::TOOL_NAME],
                        ToolLoopLimits::default(),
                    )
                });
                let revoker = registry.clone();
                let looped = run_loop(
                    &handle,
                    run,
                    tools,
                    vec![calling(&[read_call(0, "call_read", arguments)], (1, 1))],
                    CancellationToken::default(),
                    move |_| {
                        if revoked {
                            revoker.revoke(workspace_read::TOOL_NAME).expect("revoke");
                        }
                    },
                );
                assert_eq!(looped.sent.len(), 1, "{case}: no result is ever sent");
                let records = evidence(&handle, run.job);
                assert!(
                    records.iter().all(|record| record.admission().is_none()),
                    "{case}: the call was never admitted, so its handler never ran"
                );
                let job = job_record(&handle, run.job);
                match refused {
                    Refused::Stream => {
                        let receipt = looped.settled.expect("settled");
                        assert_eq!(receipt.result, None, "{case}");
                        assert!(
                            job.unknown_at.is_some() && job.stop_confirmed_at.is_some(),
                            "{case}"
                        );
                        assert!(records.is_empty(), "{case}");
                    }
                    Refused::Dispatch(kind) => {
                        let (_, stop) = stopped(looped);
                        let ToolLoopStop::Refused(refusal) = stop else {
                            panic!("{case}: expected a refusal, got {stop:?}");
                        };
                        assert_eq!(refusal.kind, kind, "{case}");
                        assert_eq!(records.len(), 1, "{case}: the refusal is recorded");
                        assert!(job.failed_at.is_some(), "{case}");
                    }
                }
                assert!(blocking(&handle, llm).is_empty(), "{case}");
            }
            worker.shutdown().expect("shutdown");
        }

        #[test]
        fn a_panicking_handler_fails_its_call_as_uncertain_and_the_loop_goes_on() {
            let (_root, directory) = data_directory("tool-handler-panic");
            let worker = DatabaseWorker::start(&directory).expect("start database");
            let handle = worker.handle();
            let (_provider, llm) = llm_setup(&handle, "model-a");
            let run = local_run(&handle, llm);
            let (registry, calls) = registered(vec![("add", Behavior::Crash)]);
            let looped = run_loop(
                &handle,
                run,
                Some(execution(&registry, &["add"], ToolLoopLimits::default())),
                vec![calling(&[add(0, "c", 1, 2)], (1, 1))],
                CancellationToken::default(),
                |_| {},
            );
            assert_eq!(calls.load(Ordering::SeqCst), 1);
            let (receipt, stop) = stopped(looped);
            assert_eq!(
                stop,
                ToolLoopStop::HandlerUncertain {
                    index: 0,
                    message: "the tool handler panicked".to_owned()
                }
            );
            assert_eq!(receipt.result, None, "an unknown outcome publishes nothing");
            let records = evidence(&handle, run.job);
            let [record] = records.as_slice() else {
                panic!("one call");
            };
            assert_eq!(
                outcome_of(record),
                Some(ToolOutcome::Uncertain {
                    cause: UncertainCause::HandlerReported,
                    message: Some("the tool handler panicked".to_owned()),
                })
            );
            let job = job_record(&handle, run.job);
            assert!(job.unknown_at.is_some() && job.stop_confirmed_at.is_some());
            assert!(blocking(&handle, llm).is_empty(), "the Profile is released");
            worker.shutdown().expect("shutdown");
        }
    }

    // ---- workspace_write_file -----------------------------------------------

    #[cfg(any(target_os = "linux", target_os = "android", target_vendor = "apple"))]
    mod workspace_write_file {
        use std::path::{Path, PathBuf};

        use super::*;
        use crate::orchestration::workspace_read::{self, Workspace};
        use crate::orchestration::workspace_write::{self, write_seam};

        const NOTES: &str = "Project: OCG\nStatus: pending\n";
        const DONE: &str = "Project: OCG\nStatus: completed\n";

        /// A Workspace holding `notes.txt`, beside the test's data directory.
        fn notes_workspace(name: &str) -> (TestRoot, PathBuf) {
            let (guard, root) = crate::test_support::temporary_directory(SUITE, name);
            std::fs::write(root.join("notes.txt"), NOTES).expect("write notes.txt");
            (guard, root)
        }

        fn edit_tools(root: &Path, directory: &DataDirectory) -> ToolExecution {
            workspace_write::edit_file_tools(
                Workspace::authorize(root, directory).expect("authorize the Workspace"),
            )
            .expect("enable the tools")
        }

        /// What an independent read of the Workspace finds at `path`.
        fn on_disk(root: &Path, path: &str) -> Option<String> {
            std::fs::read_to_string(root.join(path)).ok()
        }

        fn read_call(index: u32, id: &str, path: &str) -> String {
            let arguments = json!({ "path": path }).to_string();
            call(index, id, workspace_read::TOOL_NAME, &arguments)
        }

        fn write_call(index: u32, id: &str, arguments: &Value) -> String {
            call(
                index,
                id,
                workspace_write::TOOL_NAME,
                &arguments.to_string(),
            )
        }

        fn update(expected: &str, content: &str) -> Value {
            json!({
                "operation": "update",
                "path": "notes.txt",
                "expected_content": expected,
                "content": content,
            })
        }

        fn create(path: &str, content: &str) -> Value {
            json!({"operation": "create", "path": path, "content": content})
        }

        fn written(operation: &str, path: &str, content: &str) -> String {
            json!({"operation": operation, "path": path, "bytes_written": content.len()})
                .to_string()
        }

        #[test]
        fn a_provider_reads_then_conditionally_updates_a_file_for_its_final_answer() {
            let (_workspace_guard, root) = notes_workspace("workspace-write-update-files");
            let (_root, directory) = data_directory("workspace-write-update");
            let worker = DatabaseWorker::start(&directory).expect("start database");
            let handle = worker.handle();
            let (_provider, llm) = llm_setup(&handle, "model-a");
            let run = local_run(&handle, llm);
            let write = tool_call(run.job, 2, 0);

            // The write is admitted durably before the file changes, and the
            // file changes before its outcome is recorded.
            let (seen_root, seam_handle) = (root.clone(), handle.clone());
            let points = Arc::new(std::sync::Mutex::new(Vec::new()));
            let reached = points.clone();
            dispatch_seam::install(move |point, call| {
                if call != write {
                    return;
                }
                let records = wait(seam_handle.tool_evidence(call.job, None, 10)).expect("read");
                let record = records.iter().find(|record| record.call == write);
                let content = on_disk(&seen_root, "notes.txt");
                match point {
                    Point::BeforeAdmission => assert!(record.is_none()),
                    Point::Admitted => {
                        let record = record.expect("admitted");
                        assert!(record.admission().is_some() && record.outcome.is_none());
                        assert_eq!(content.as_deref(), Some(NOTES), "nothing written yet");
                    }
                    Point::Ran => {
                        assert!(record.expect("admitted").outcome.is_none());
                        assert_eq!(content.as_deref(), Some(DONE), "written, not recorded");
                    }
                }
                reached.lock().expect("lock").push(point);
            });
            let looped = run_loop(
                &handle,
                run,
                Some(edit_tools(&root, &directory)),
                vec![
                    calling(&[read_call(0, "call_read", "notes.txt")], (20, 5)),
                    calling(
                        &[write_call(0, "call_write", &update(NOTES, DONE))],
                        (60, 30),
                    ),
                    answering("notes.txt now says Status: completed.", (90, 9)),
                ],
                CancellationToken::default(),
                |_| {},
            );
            let receipt = looped.settled.expect("the loop settles");
            assert_eq!(receipt.job, run.job);
            assert_eq!(
                *points.lock().expect("lock"),
                [Point::BeforeAdmission, Point::Admitted, Point::Ran]
            );
            assert_eq!(on_disk(&root, "notes.txt").as_deref(), Some(DONE));

            let [first, second, third] = looped.sent.as_slice() else {
                panic!("three Provider requests");
            };
            assert_eq!(
                first
                    .tools
                    .iter()
                    .map(|tool| tool.function.name.as_str())
                    .collect::<Vec<_>>(),
                [workspace_read::TOOL_NAME, workspace_write::TOOL_NAME]
            );
            let read_result = json!({"path": "notes.txt", "content": NOTES}).to_string();
            assert_eq!(
                answer_to(second, "call_read").as_deref(),
                Some(read_result.as_str())
            );
            let write_result = written("update", "notes.txt", DONE);
            assert_eq!(
                answer_to(third, "call_write").as_deref(),
                Some(write_result.as_str()),
                "the continuation carries the recorded, compact result"
            );

            let records = evidence(&handle, run.job);
            let [read, write_record] = records.as_slice() else {
                panic!("two calls: {records:?}");
            };
            assert_eq!(read.call, tool_call(run.job, 1, 0));
            let read_admission = read.admission().expect("admitted");
            assert_eq!(read_admission.effect, ToolEffect::ReadOnly);
            assert_eq!(outcome_of(read), Some(completed(&read_result)));
            assert_eq!(write_record.call, write);
            assert_eq!(
                (
                    write_record.tool_call_id.as_str(),
                    write_record.name.as_str()
                ),
                ("call_write", workspace_write::TOOL_NAME)
            );
            let admission = write_record.admission().expect("admitted");
            assert_eq!(admission.handler_version, "workspace_write_file-1");
            assert_eq!(admission.effect, ToolEffect::NonIdempotent);
            assert_eq!(
                serde_json::from_str::<Value>(admission.arguments.as_str()).expect("JSON"),
                update(NOTES, DONE),
                "the evidence keeps the complete arguments, contents included"
            );
            assert_eq!(outcome_of(write_record), Some(completed(&write_result)));
            assert!(write_record.is_applied() && !write_record.is_unresolved());

            let job = job_record(&handle, run.job);
            assert!(job.completed_at.is_some());
            let outputs = wait(handle.job_outputs(run.job, None, 100)).expect("outputs");
            assert_eq!(
                outputs.last().and_then(|output| output.summary.as_deref()),
                Some("notes.txt now says Status: completed.")
            );
            assert!(blocking(&handle, llm).is_empty(), "the Profile is released");
            let samples = TokenRequestSample::per_request(&job_metrics(&handle, run.job))
                .expect("per request");
            assert_eq!(
                samples
                    .iter()
                    .map(|sample| (sample.request, sample.total))
                    .collect::<Vec<_>>(),
                [
                    (number(1), Some(25)),
                    (number(2), Some(90)),
                    (number(3), Some(99))
                ]
            );

            // Repeating a known write needs an explicit acknowledgement.
            assert_eq!(
                violation(wait(handle.retry_job(retry_of(run.job, llm, false)))),
                Violation::RepeatedEffectNotAcknowledged(write)
            );
            // A restart replays nothing and keeps what was recorded.
            std::fs::write(root.join("notes.txt"), NOTES).expect("restore the file");
            worker.shutdown().expect("shutdown");
            let worker = DatabaseWorker::start(&directory).expect("restart");
            let handle = worker.handle();
            assert!(handle.abandoned_at_open().is_empty());
            assert_eq!(evidence(&handle, run.job), records);
            assert_eq!(on_disk(&root, "notes.txt").as_deref(), Some(NOTES));
            wait(handle.retry_job(retry_of(run.job, llm, true))).expect("an acknowledged retry");
            worker.shutdown().expect("shutdown");
            assert_relationships_consistent(&directory);
        }

        #[test]
        fn a_provider_creates_a_new_file_and_never_replaces_an_existing_one() {
            let (_workspace_guard, root) = notes_workspace("workspace-write-create-files");
            let (_root, directory) = data_directory("workspace-write-create");
            let worker = DatabaseWorker::start(&directory).expect("start database");
            let handle = worker.handle();
            let (_provider, llm) = llm_setup(&handle, "model-a");

            let run = local_run(&handle, llm);
            let summary = "Summary: done.\n";
            let looped = run_loop(
                &handle,
                run,
                Some(edit_tools(&root, &directory)),
                vec![
                    calling(
                        &[write_call(
                            0,
                            "call_create",
                            &create("summary.txt", summary),
                        )],
                        (10, 5),
                    ),
                    answering("Created summary.txt.", (20, 4)),
                ],
                CancellationToken::default(),
                |_| {},
            );
            looped.settled.expect("the loop settles");
            assert_eq!(on_disk(&root, "summary.txt").as_deref(), Some(summary));
            let [record] = evidence(&handle, run.job).try_into().expect("one call");
            assert_eq!(
                outcome_of(&record),
                Some(completed(&written("create", "summary.txt", summary)))
            );
            assert!(job_record(&handle, run.job).completed_at.is_some());

            // A second create of the same name is refused and changes nothing.
            let run = local_run(&handle, llm);
            let looped = run_loop(
                &handle,
                run,
                Some(edit_tools(&root, &directory)),
                vec![
                    calling(
                        &[write_call(
                            0,
                            "call_create",
                            &create("summary.txt", "other\n"),
                        )],
                        (10, 5),
                    ),
                    answering("never sent", (1, 1)),
                ],
                CancellationToken::default(),
                |_| {},
            );
            assert_eq!(looped.sent.len(), 1, "no result is sent");
            let (_, stop) = stopped(looped);
            let message = "something already exists at the path".to_owned();
            assert_eq!(
                stop,
                ToolLoopStop::HandlerFailed {
                    index: 0,
                    message: message.clone()
                }
            );
            let [record] = evidence(&handle, run.job).try_into().expect("one call");
            assert_eq!(
                outcome_of(&record),
                Some(ToolOutcome::NotApplied { message })
            );
            assert!(!record.is_applied() && !record.is_unresolved());
            assert_eq!(on_disk(&root, "summary.txt").as_deref(), Some(summary));
            assert!(job_record(&handle, run.job).failed_at.is_some());
            wait(handle.retry_job(retry_of(run.job, llm, false)))
                .expect("nothing was applied, so nothing repeats");
            worker.shutdown().expect("shutdown");
        }

        #[test]
        fn a_failed_precondition_is_not_applied_and_fails_the_job() {
            let (_workspace_guard, root) = notes_workspace("workspace-write-mismatch-files");
            let (_root, directory) = data_directory("workspace-write-mismatch");
            let worker = DatabaseWorker::start(&directory).expect("start database");
            let handle = worker.handle();
            let (_provider, llm) = llm_setup(&handle, "model-a");
            let run = local_run(&handle, llm);
            let stale = "Project: OCG\nStatus: pending";
            let looped = run_loop(
                &handle,
                run,
                Some(edit_tools(&root, &directory)),
                vec![
                    calling(
                        &[write_call(0, "call_write", &update(stale, DONE))],
                        (10, 5),
                    ),
                    answering("never sent", (1, 1)),
                ],
                CancellationToken::default(),
                |_| {},
            );
            assert_eq!(looped.sent.len(), 1);
            let (receipt, stop) = stopped(looped);
            let message = "the file's current content differs from expected_content".to_owned();
            assert_eq!(
                stop,
                ToolLoopStop::HandlerFailed {
                    index: 0,
                    message: message.clone()
                }
            );
            assert!(receipt.result.is_some());
            let [record] = evidence(&handle, run.job).try_into().expect("one call");
            assert_eq!(
                outcome_of(&record),
                Some(ToolOutcome::NotApplied { message })
            );
            assert_eq!(on_disk(&root, "notes.txt").as_deref(), Some(NOTES));
            assert!(job_record(&handle, run.job).failed_at.is_some());
            assert!(blocking(&handle, llm).is_empty());
            wait(handle.retry_job(retry_of(run.job, llm, false))).expect("nothing to repeat");
            worker.shutdown().expect("shutdown");
        }

        #[test]
        fn a_failure_after_publication_is_uncertain_and_blocks_a_retry() {
            let (_workspace_guard, root) = notes_workspace("workspace-write-uncertain-files");
            let (_root, directory) = data_directory("workspace-write-uncertain");
            let worker = DatabaseWorker::start(&directory).expect("start database");
            let handle = worker.handle();
            let (_provider, llm) = llm_setup(&handle, "model-a");
            let run = local_run(&handle, llm);
            write_seam::install(|point| point != write_seam::Point::Published);
            let looped = run_loop(
                &handle,
                run,
                Some(edit_tools(&root, &directory)),
                vec![
                    calling(
                        &[write_call(0, "call_write", &update(NOTES, DONE))],
                        (10, 5),
                    ),
                    answering("never sent", (1, 1)),
                ],
                CancellationToken::default(),
                |_| {},
            );
            write_seam::clear();
            assert_eq!(looped.sent.len(), 1, "an uncertain write sends no result");
            let (receipt, stop) = stopped(looped);
            let message =
                "the new content was published, but its durability could not be confirmed";
            assert_eq!(
                stop,
                ToolLoopStop::HandlerUncertain {
                    index: 0,
                    message: message.to_owned()
                }
            );
            assert_eq!(receipt.result, None);
            let [record] = evidence(&handle, run.job).try_into().expect("one call");
            assert_eq!(
                outcome_of(&record),
                Some(ToolOutcome::Uncertain {
                    cause: UncertainCause::HandlerReported,
                    message: Some(message.to_owned()),
                })
            );
            assert!(record.is_unresolved());
            assert_eq!(on_disk(&root, "notes.txt").as_deref(), Some(DONE));
            let job = job_record(&handle, run.job);
            assert!(job.unknown_at.is_some() && job.stop_confirmed_at.is_some());
            assert_eq!(
                violation(wait(handle.retry_job(retry_of(run.job, llm, true)))),
                Violation::UnresolvedToolEffect(tool_call(run.job, 1, 0))
            );
            worker.shutdown().expect("shutdown");
        }

        #[test]
        fn a_write_whose_outcome_cannot_be_recorded_is_never_reported_as_done() {
            let (_workspace_guard, root) = notes_workspace("workspace-write-unrecorded-files");
            let (_root, directory) = data_directory("workspace-write-unrecorded");
            let worker = DatabaseWorker::start_with_config(&directory, lenient()).expect("start");
            let handle = worker.handle();
            let (_provider, llm) = llm_setup(&handle, "model-a");
            let run = local_run(&handle, llm);
            let (seam_handle, path) = (handle.clone(), directory.database_path().to_path_buf());
            dispatch_seam::install(move |point, _| {
                if point == Point::Ran {
                    protect(&seam_handle, &path);
                }
            });
            let looped = run_loop(
                &handle,
                run,
                Some(edit_tools(&root, &directory)),
                vec![
                    calling(
                        &[write_call(0, "call_write", &update(NOTES, DONE))],
                        (10, 5),
                    ),
                    answering("never sent", (1, 1)),
                ],
                CancellationToken::default(),
                |_| {},
            );
            assert_eq!(looped.sent.len(), 1, "an unrecorded result is never sent");
            match looped.settled {
                Err(ExecutionRuntimeError::ToolLoopNotSettled { stop, .. }) => assert_eq!(
                    *stop,
                    ToolLoopStop::EvidenceNotRecorded {
                        index: 0,
                        outcome: Some(ToolOutcomeKind::Completed),
                    }
                ),
                other => panic!("the unrecorded outcome must be reported, got {other:?}"),
            }
            assert_eq!(on_disk(&root, "notes.txt").as_deref(), Some(DONE));
            worker.shutdown().expect("shutdown");

            let worker = DatabaseWorker::start_with_config(&directory, lenient()).expect("restart");
            let handle = worker.handle();
            assert_eq!(handle.abandoned_at_open(), [run.job]);
            let job = job_record(&handle, run.job);
            assert!(job.unknown_at.is_some() && job.completed_at.is_none());
            assert!(job.stop_confirmed_at.is_some(), "no call spawns a process");
            let [record] = evidence(&handle, run.job).try_into().expect("one call");
            assert!(record.outcome.is_none() && record.is_unresolved());
            assert_eq!(
                violation(wait(handle.retry_job(retry_of(run.job, llm, true)))),
                // Acknowledging does not lift an effect that may have happened.
                Violation::UnresolvedToolEffect(record.call)
            );
            worker.shutdown().expect("shutdown");
            assert_relationships_consistent(&directory);
        }

        #[test]
        fn a_cancellation_after_the_admission_writes_nothing() {
            let (_workspace_guard, root) = notes_workspace("workspace-write-cancelled-files");
            let (_root, directory) = data_directory("workspace-write-cancelled");
            let worker = DatabaseWorker::start(&directory).expect("start database");
            let handle = worker.handle();
            let (_provider, llm) = llm_setup(&handle, "model-a");
            let run = local_run(&handle, llm);
            let cancellation = CancellationToken::default();
            let canceller = cancellation.clone();
            dispatch_seam::install(move |point, _| {
                if point == Point::Admitted {
                    canceller.cancel();
                }
            });
            let looped = run_loop(
                &handle,
                run,
                Some(edit_tools(&root, &directory)),
                vec![calling(
                    &[write_call(0, "call_write", &update(NOTES, DONE))],
                    (10, 5),
                )],
                cancellation,
                |_| {},
            );
            let (_, stop) = stopped(looped);
            assert_eq!(stop, ToolLoopStop::Cancelled);
            let [record] = evidence(&handle, run.job).try_into().expect("one call");
            assert_eq!(
                outcome_of(&record),
                Some(ToolOutcome::NotInvoked {
                    reason: NotInvokedReason::Cancelled
                })
            );
            assert_eq!(on_disk(&root, "notes.txt").as_deref(), Some(NOTES));
            let job = job_record(&handle, run.job);
            assert!(job.unknown_at.is_some() && job.stop_confirmed_at.is_some());
            wait(handle.retry_job(retry_of(run.job, llm, false))).expect("nothing ran");
            worker.shutdown().expect("shutdown");
        }

        #[test]
        fn a_crash_after_the_write_leaves_it_unresolved_and_a_restart_replays_nothing() {
            let (_workspace_guard, root) = notes_workspace("workspace-write-crash-files");
            let (_root, directory) = data_directory("workspace-write-crash");
            let worker = DatabaseWorker::start(&directory).expect("start database");
            let handle = worker.handle();
            let (_provider, llm) = llm_setup(&handle, "model-a");
            let run = local_run(&handle, llm);
            let tools = edit_tools(&root, &directory);
            let loop_handle = handle.clone();
            let died = std::thread::spawn(move || {
                dispatch_seam::install(|point, _| {
                    assert_ne!(point, Point::Ran, "the process died here");
                });
                run_loop(
                    &loop_handle,
                    run,
                    Some(tools),
                    vec![
                        calling(
                            &[write_call(0, "call_write", &update(NOTES, DONE))],
                            (10, 5),
                        ),
                        answering("never sent", (1, 1)),
                    ],
                    CancellationToken::default(),
                    |_| {},
                );
            })
            .join();
            assert!(died.is_err(), "the process died after the write");
            assert_eq!(on_disk(&root, "notes.txt").as_deref(), Some(DONE));
            worker.shutdown().expect("shutdown");

            // The file is put back; no restart may write it again.
            std::fs::write(root.join("notes.txt"), NOTES).expect("restore the file");
            for _ in 0..2 {
                let worker = DatabaseWorker::start(&directory).expect("restart");
                let handle = worker.handle();
                let job = job_record(&handle, run.job);
                assert!(job.unknown_at.is_some() && job.stop_confirmed_at.is_some());
                assert!(blocking(&handle, llm).is_empty());
                let [record] = evidence(&handle, run.job).try_into().expect("one call");
                assert!(
                    record.admission().is_some()
                        && record.outcome.is_none()
                        && record.is_unresolved()
                );
                assert_eq!(
                    violation(wait(handle.retry_job(retry_of(run.job, llm, true)))),
                    // The unresolved effect stays, however often it restarts.
                    Violation::UnresolvedToolEffect(record.call)
                );
                worker.shutdown().expect("shutdown");
                assert_eq!(on_disk(&root, "notes.txt").as_deref(), Some(NOTES));
            }
            assert_relationships_consistent(&directory);
        }
    }

    // ---- workspace_run_command ----------------------------------------------

    #[cfg(any(target_os = "linux", target_os = "android", target_vendor = "apple"))]
    mod workspace_run_command {
        use std::os::unix::fs::PermissionsExt;
        use std::path::PathBuf;
        use std::time::Duration;

        use super::*;
        use crate::orchestration::workspace_command::{self, CommandEnvironment};
        use crate::orchestration::workspace_read::{self, Workspace};
        use crate::orchestration::workspace_write;

        const BUGGY: &str = "pub fn add(a: i32, b: i32) -> i32 {\n    a - b\n}\n";
        const FIXED: &str = "pub fn add(a: i32, b: i32) -> i32 {\n    a + b\n}\n";

        /// A fake `cargo` that fails its test until `src/lib.rs` adds, and
        /// logs every run beside itself, outside the Workspace.
        const TESTING: &str = "echo run >> \"$0.log\"\n\
            if grep -q 'a + b' src/lib.rs; then\n\
              echo 'test tests::adds_two_numbers ... ok'\n\
              echo 'test result: ok. 1 passed; 0 failed'\n\
              exit 0\n\
            fi\n\
            echo 'test tests::adds_two_numbers ... FAILED'\n\
            echo 'test result: FAILED. 0 passed; 1 failed'\n\
            echo 'error: test failed, to rerun pass `--lib`' >&2\n\
            exit 101";

        /// A fake `cargo` that never finishes, with a process of its group.
        const HANGING: &str = "echo run >> \"$0.log\"\n\
            echo $$ > leader.tmp && mv leader.tmp leader.pid\n\
            sleep 60 &\n\
            echo $! > child.tmp && mv child.tmp child.pid\n\
            touch ready\n\
            wait";

        /// A Workspace holding a buggy `src/lib.rs`, and a directory holding
        /// a fake `cargo` that runs `script`.
        struct Project {
            root: PathBuf,
            bin: PathBuf,
            _guards: [TestRoot; 2],
        }

        fn setup(name: &str, script: &str) -> Project {
            let (workspace_guard, root) =
                crate::test_support::temporary_directory(SUITE, &format!("{name}-files"));
            let (bin_guard, bin) =
                crate::test_support::temporary_directory(SUITE, &format!("{name}-bin"));
            std::fs::create_dir(root.join("src")).expect("create src");
            std::fs::write(root.join("src/lib.rs"), BUGGY).expect("write src/lib.rs");
            let cargo = bin.join("cargo");
            std::fs::write(
                &cargo,
                format!("#!/bin/sh\n[ \"$1\" = warm ] && exit 0\n{script}\n"),
            )
            .expect("write cargo");
            std::fs::set_permissions(&cargo, std::fs::Permissions::from_mode(0o755))
                .expect("make it executable");
            // A new executable's first start may be slow, as on macOS; it
            // must not count against a limit.
            let warmed = std::process::Command::new(&cargo)
                .arg("warm")
                .status()
                .expect("start it");
            assert!(warmed.success());
            Project {
                root,
                bin,
                _guards: [workspace_guard, bin_guard],
            }
        }

        impl Project {
            fn tools(&self, directory: &DataDirectory, timeout: Duration) -> ToolExecution {
                let path = format!("{}:/usr/bin:/bin", self.bin.display());
                workspace_command::code_tools_with(
                    Workspace::authorize(&self.root, directory).expect("authorize"),
                    CommandEnvironment::from_source(move |name| {
                        (name == "PATH").then(|| path.clone().into())
                    }),
                    timeout,
                )
                .expect("enable the tools")
            }

            /// How many times the fake `cargo` ran.
            fn runs(&self) -> usize {
                std::fs::read_to_string(self.bin.join("cargo.log"))
                    .map_or(0, |log| log.lines().count())
            }

            fn source(&self) -> Option<String> {
                std::fs::read_to_string(self.root.join("src/lib.rs")).ok()
            }

            /// Whether every process the hanging `cargo` started is gone.
            fn stopped(&self) -> bool {
                ["leader.pid", "child.pid"].iter().all(|file| {
                    let text = std::fs::read_to_string(self.root.join(file)).expect("a pid");
                    let pid = rustix::process::Pid::from_raw(text.trim().parse().expect("a pid"))
                        .expect("a positive pid");
                    rustix::process::test_kill_process(pid) == Err(rustix::io::Errno::SRCH)
                })
            }
        }

        fn until(mut condition: impl FnMut() -> bool) {
            let deadline = std::time::Instant::now() + crate::test_support::WAIT;
            while !condition() {
                assert!(std::time::Instant::now() < deadline, "not reached in time");
                std::thread::sleep(Duration::from_millis(5));
            }
        }

        fn test_call(index: u32, id: &str) -> String {
            call(
                index,
                id,
                workspace_command::TOOL_NAME,
                r#"{"command":"cargo_test"}"#,
            )
        }

        fn test_result(passed: bool) -> Value {
            if passed {
                json!({
                    "command": "cargo_test",
                    "exit_code": 0,
                    "stdout": "test result: ok. 1 passed; 0 failed",
                    "stderr": "",
                    "truncated": false,
                })
            } else {
                json!({
                    "command": "cargo_test",
                    "exit_code": 101,
                    "stdout": "test result: FAILED. 0 passed; 1 failed\ntests::adds_two_numbers",
                    "stderr": "",
                    "truncated": false,
                })
            }
        }

        fn parsed(text: &str) -> Value {
            serde_json::from_str(text).expect("JSON")
        }

        /// One Job that runs the hanging `cargo` and is stopped.
        fn hanging_run(
            handle: &DatabaseHandle,
            llm: ProfileId,
            tools: ToolExecution,
            cancellation: CancellationToken,
        ) -> (TokenRun, Looped) {
            let run = local_run(handle, llm);
            let looped = run_loop(
                handle,
                run,
                Some(tools),
                vec![
                    calling(&[test_call(0, "call_test")], (10, 5)),
                    answering("never sent", (1, 1)),
                ],
                cancellation,
                |_| {},
            );
            (run, looped)
        }

        #[test]
        fn a_provider_reads_sees_a_failing_test_fixes_the_code_and_sees_it_pass() {
            let project = setup("command-loop", TESTING);
            let (_root, directory) = data_directory("command-loop");
            let worker = DatabaseWorker::start(&directory).expect("start database");
            let handle = worker.handle();
            let (_provider, llm) = llm_setup(&handle, "model-a");
            let run = local_run(&handle, llm);
            let first_test = tool_call(run.job, 2, 0);

            // The command is admitted durably before it runs, and runs
            // before its outcome is recorded.
            let (seam_handle, log) = (handle.clone(), project.bin.join("cargo.log"));
            let points = Arc::new(std::sync::Mutex::new(Vec::new()));
            let reached = points.clone();
            dispatch_seam::install(move |point, call| {
                if call != first_test {
                    return;
                }
                let records = wait(seam_handle.tool_evidence(call.job, None, 10)).expect("read");
                let record = records.iter().find(|record| record.call == first_test);
                let runs = std::fs::read_to_string(&log).map_or(0, |log| log.lines().count());
                match point {
                    Point::BeforeAdmission => assert!(record.is_none() && runs == 0),
                    Point::Admitted => {
                        let record = record.expect("admitted");
                        assert!(record.admission().is_some() && record.outcome.is_none());
                        assert_eq!(runs, 0, "nothing ran yet");
                    }
                    Point::Ran => {
                        assert!(record.expect("admitted").outcome.is_none());
                        assert_eq!(runs, 1, "ran, not recorded");
                    }
                }
                reached.lock().expect("lock").push(point);
            });
            let fix = json!({
                "operation": "update",
                "path": "src/lib.rs",
                "expected_content": BUGGY,
                "content": FIXED,
            });
            let looped = run_loop(
                &handle,
                run,
                Some(project.tools(&directory, crate::test_support::WAIT)),
                vec![
                    calling(
                        &[call(
                            0,
                            "call_read",
                            workspace_read::TOOL_NAME,
                            r#"{"path":"src/lib.rs"}"#,
                        )],
                        (20, 5),
                    ),
                    calling(&[test_call(0, "call_test_1")], (60, 10)),
                    calling(
                        &[call(
                            0,
                            "call_fix",
                            workspace_write::TOOL_NAME,
                            &fix.to_string(),
                        )],
                        (120, 40),
                    ),
                    calling(&[test_call(0, "call_test_2")], (200, 10)),
                    answering(
                        "add subtracted; it adds now and cargo test passes.",
                        (260, 12),
                    ),
                ],
                CancellationToken::default(),
                |_| {},
            );
            let receipt = looped.settled.expect("the loop settles");
            assert_eq!(receipt.job, run.job);
            assert_eq!(
                *points.lock().expect("lock"),
                [Point::BeforeAdmission, Point::Admitted, Point::Ran]
            );
            assert_eq!(project.source().as_deref(), Some(FIXED));
            assert_eq!(project.runs(), 2);

            // The failing test is read by the Provider, and the Job goes on.
            let [first, .., third, _, fifth] = looped.sent.as_slice() else {
                panic!("five Provider requests: {}", looped.sent.len());
            };
            assert_eq!(
                first
                    .tools
                    .iter()
                    .map(|tool| tool.function.name.as_str())
                    .collect::<Vec<_>>(),
                [
                    workspace_read::TOOL_NAME,
                    workspace_write::TOOL_NAME,
                    workspace_command::TOOL_NAME,
                    crate::orchestration::workspace_discovery::LIST_TOOL_NAME,
                    crate::orchestration::workspace_discovery::SEARCH_TOOL_NAME,
                    crate::orchestration::workspace_code::TOOL_NAME,
                    crate::orchestration::workspace_fts::TOOL_NAME,
                ]
            );
            let failed = answer_to(third, "call_test_1").expect("answered");
            assert_eq!(parsed(&failed), test_result(false));
            let passed = answer_to(fifth, "call_test_2").expect("answered");
            assert_eq!(parsed(&passed), test_result(true));

            let records = evidence(&handle, run.job);
            let [read, test_1, write, test_2] = records.as_slice() else {
                panic!("four calls: {records:?}");
            };
            assert_eq!(
                read.admission().expect("admitted").effect,
                ToolEffect::ReadOnly
            );
            assert_eq!(
                write.admission().expect("admitted").effect,
                ToolEffect::NonIdempotent
            );
            for (record, request, id, result) in [
                (test_1, 2, "call_test_1", &failed),
                (test_2, 4, "call_test_2", &passed),
            ] {
                assert_eq!(record.call, tool_call(run.job, request, 0));
                assert_eq!(
                    (record.tool_call_id.as_str(), record.name.as_str()),
                    (id, workspace_command::TOOL_NAME)
                );
                let admission = record.admission().expect("admitted");
                assert_eq!(admission.handler_version, "workspace_run_command-4");
                assert_eq!(admission.effect, ToolEffect::NonIdempotent);
                assert_eq!(admission.arguments.as_str(), r#"{"command":"cargo_test"}"#);
                assert_eq!(
                    outcome_of(record),
                    Some(completed(result)),
                    "the result sent is the result recorded"
                );
                assert!(record.is_applied() && !record.is_unresolved());
            }

            let job = job_record(&handle, run.job);
            assert!(job.completed_at.is_some());
            let outputs = wait(handle.job_outputs(run.job, None, 100)).expect("outputs");
            assert_eq!(
                outputs.last().and_then(|output| output.summary.as_deref()),
                Some("add subtracted; it adds now and cargo test passes.")
            );
            assert!(blocking(&handle, llm).is_empty(), "the Profile is released");
            let samples = TokenRequestSample::per_request(&job_metrics(&handle, run.job))
                .expect("per request");
            assert_eq!(
                samples
                    .iter()
                    .map(|sample| (sample.request, sample.total))
                    .collect::<Vec<_>>(),
                [
                    (number(1), Some(25)),
                    (number(2), Some(70)),
                    (number(3), Some(160)),
                    (number(4), Some(210)),
                    (number(5), Some(272)),
                ]
            );

            // Running the commands again needs an explicit acknowledgement.
            assert_eq!(
                violation(wait(handle.retry_job(retry_of(run.job, llm, false)))),
                Violation::RepeatedEffectNotAcknowledged(tool_call(run.job, 2, 0))
            );
            // A restart replays nothing and keeps what was recorded.
            worker.shutdown().expect("shutdown");
            let worker = DatabaseWorker::start(&directory).expect("restart");
            let handle = worker.handle();
            assert!(handle.abandoned_at_open().is_empty());
            assert_eq!(evidence(&handle, run.job), records);
            assert_eq!(project.runs(), 2);
            worker.shutdown().expect("shutdown");
            assert_relationships_consistent(&directory);
        }

        #[test]
        fn a_command_past_its_own_limit_is_stopped_uncertain_and_blocks_a_retry() {
            let project = setup("command-own-limit", HANGING);
            let (_root, directory) = data_directory("command-own-limit");
            let worker = DatabaseWorker::start(&directory).expect("start database");
            let handle = worker.handle();
            let (_provider, llm) = llm_setup(&handle, "model-a");
            let tools = project.tools(&directory, Duration::from_secs(1));
            let (run, looped) = hanging_run(&handle, llm, tools, CancellationToken::default());
            assert_eq!(looped.sent.len(), 1, "an uncertain command sends no result");
            let (receipt, stop) = stopped(looped);
            let message = "the command did not finish within 1s and was stopped";
            assert_eq!(
                stop,
                ToolLoopStop::HandlerUncertain {
                    index: 0,
                    message: message.to_owned()
                }
            );
            assert_eq!(receipt.result, None);
            let [record] = evidence(&handle, run.job).try_into().expect("one call");
            assert_eq!(
                outcome_of(&record),
                Some(ToolOutcome::Uncertain {
                    cause: UncertainCause::HandlerReported,
                    message: Some(message.to_owned()),
                })
            );
            assert!(record.is_unresolved());
            until(|| project.stopped());
            let job = job_record(&handle, run.job);
            assert!(job.unknown_at.is_some() && job.stop_confirmed_at.is_some());
            assert!(blocking(&handle, llm).is_empty());
            assert_eq!(
                violation(wait(handle.retry_job(retry_of(run.job, llm, true)))),
                Violation::UnresolvedToolEffect(record.call)
            );
            assert_eq!(project.runs(), 1);
            worker.shutdown().expect("shutdown");
        }

        #[test]
        fn a_command_waiting_for_capacity_gets_both_limits_after_process_start() {
            use crate::orchestration::blocking::{Kind, Permit};
            let project = setup("command-permit-limits", TESTING);
            let (_root, directory) = data_directory("command-permit-limits");
            let worker = DatabaseWorker::start(&directory).expect("database");
            let handle = worker.handle();
            let (_, llm) = llm_setup(&handle, "model-a");
            let run = local_run(&handle, llm);
            let mut tools = project.tools(&directory, Duration::from_millis(500));
            tools.limits.handler_timeout = Duration::from_millis(300);
            let permit = Permit::acquire(Kind::Command);
            let database = handle.clone();
            let executing = std::thread::spawn(move || {
                run_loop(
                    &database,
                    run,
                    Some(tools),
                    vec![
                        calling(&[test_call(0, "call_test")], (10, 5)),
                        answering("finished after capacity", (2, 1)),
                    ],
                    CancellationToken::default(),
                    |_| {},
                )
            });
            until(|| !evidence(&handle, run.job).is_empty());
            std::thread::sleep(Duration::from_millis(800));
            assert_eq!(
                project.runs(),
                0,
                "the permit still belongs to another command"
            );
            drop(permit);
            let looped = executing.join().expect("command task");
            looped
                .settled
                .expect("neither limit counted the permit wait");
            let [record] = evidence(&handle, run.job).try_into().expect("one call");
            assert!(matches!(
                outcome_of(&record),
                Some(ToolOutcome::Completed { .. })
            ));
            assert_eq!(project.runs(), 1);
            worker.shutdown().expect("shutdown");
        }

        #[test]
        fn cancelling_a_command_waiting_for_capacity_is_not_applied_and_allows_retry() {
            use crate::orchestration::blocking::{Kind, Permit};
            let project = setup("command-permit-cancel", TESTING);
            let (_root, directory) = data_directory("command-permit-cancel");
            let worker = DatabaseWorker::start(&directory).expect("database");
            let handle = worker.handle();
            let (_, llm) = llm_setup(&handle, "model-a");
            let run = local_run(&handle, llm);
            let tools = project.tools(&directory, Duration::from_millis(300));
            let permit = Permit::acquire(Kind::Command);
            let cancellation = CancellationToken::default();
            let cancelling = cancellation.clone();
            let database = handle.clone();
            let executing = std::thread::spawn(move || {
                run_loop(
                    &database,
                    run,
                    Some(tools),
                    vec![calling(&[test_call(0, "call_test")], (10, 5))],
                    cancellation,
                    |_| {},
                )
            });
            until(|| !evidence(&handle, run.job).is_empty());
            cancelling.cancel();
            until(|| executing.is_finished());
            let (_, stop) = stopped(executing.join().expect("command task"));
            assert_eq!(stop, ToolLoopStop::Cancelled);
            let [record] = evidence(&handle, run.job).try_into().expect("one call");
            assert!(matches!(
                outcome_of(&record),
                Some(
                    ToolOutcome::NotApplied { .. }
                        | ToolOutcome::NotInvoked {
                            reason: NotInvokedReason::Cancelled
                        }
                )
            ));
            assert!(!record.is_unresolved());
            assert_eq!(
                project.runs(),
                0,
                "cancellation cannot start a queued command"
            );
            wait(handle.retry_job(retry_of(run.job, llm, false)))
                .expect("retry without repeated effects");
            drop(permit);
            std::thread::sleep(Duration::from_millis(50));
            assert_eq!(
                project.runs(),
                0,
                "the cancelled waiter stays stopped after capacity returns"
            );
            worker.shutdown().expect("shutdown");
        }

        #[test]
        fn a_command_past_the_tool_loop_limit_is_stopped_uncertain_and_blocks_a_retry() {
            let project = setup("command-loop-limit", HANGING);
            let (_root, directory) = data_directory("command-loop-limit");
            let worker = DatabaseWorker::start(&directory).expect("start database");
            let handle = worker.handle();
            let (_provider, llm) = llm_setup(&handle, "model-a");
            let mut tools = project.tools(&directory, crate::test_support::WAIT);
            tools.limits.handler_timeout = Duration::from_secs(1);
            let (run, looped) = hanging_run(&handle, llm, tools, CancellationToken::default());
            let (_, stop) = stopped(looped);
            assert_eq!(stop, ToolLoopStop::TimedOut { index: 0 });
            let [record] = evidence(&handle, run.job).try_into().expect("one call");
            assert_eq!(
                outcome_of(&record),
                Some(ToolOutcome::Uncertain {
                    cause: UncertainCause::TimedOut,
                    message: None,
                })
            );
            until(|| project.stopped());
            let job = job_record(&handle, run.job);
            assert!(job.unknown_at.is_some() && job.stop_confirmed_at.is_some());
            assert_eq!(
                violation(wait(handle.retry_job(retry_of(run.job, llm, true)))),
                Violation::UnresolvedToolEffect(record.call)
            );
            worker.shutdown().expect("shutdown");
        }

        #[test]
        fn a_cancellation_while_a_command_runs_stops_it_and_leaves_it_uncertain() {
            let project = setup("command-cancelled", HANGING);
            let (_root, directory) = data_directory("command-cancelled");
            let worker = DatabaseWorker::start(&directory).expect("start database");
            let handle = worker.handle();
            let (_provider, llm) = llm_setup(&handle, "model-a");
            let cancellation = CancellationToken::default();
            let (canceller, ready) = (cancellation.clone(), project.root.join("ready"));
            let cancelling = std::thread::spawn(move || {
                until(|| ready.exists());
                canceller.cancel();
            });
            let tools = project.tools(&directory, crate::test_support::WAIT);
            let (run, looped) = hanging_run(&handle, llm, tools, cancellation);
            cancelling.join().expect("cancelled");
            let (_, stop) = stopped(looped);
            assert_eq!(stop, ToolLoopStop::Cancelled);
            let [record] = evidence(&handle, run.job).try_into().expect("one call");
            assert_eq!(
                outcome_of(&record),
                Some(ToolOutcome::Uncertain {
                    cause: UncertainCause::Cancelled,
                    message: None,
                })
            );
            until(|| project.stopped());
            let job = job_record(&handle, run.job);
            assert!(job.unknown_at.is_some() && job.stop_confirmed_at.is_some());
            assert_eq!(
                violation(wait(handle.retry_job(retry_of(run.job, llm, true)))),
                Violation::UnresolvedToolEffect(record.call)
            );
            worker.shutdown().expect("shutdown");
        }

        #[test]
        fn a_command_refused_before_it_starts_runs_nothing() {
            let project = setup("command-refused", TESTING);
            let (_root, directory) = data_directory("command-refused");
            let worker = DatabaseWorker::start(&directory).expect("start database");
            let handle = worker.handle();
            let (_provider, llm) = llm_setup(&handle, "model-a");

            // Arguments the schema refuses are never a command.
            let run = local_run(&handle, llm);
            let looped = run_loop(
                &handle,
                run,
                Some(project.tools(&directory, crate::test_support::WAIT)),
                vec![
                    calling(
                        &[call(
                            0,
                            "call_shell",
                            workspace_command::TOOL_NAME,
                            r#"{"command":"cargo_test","args":["; touch ran"]}"#,
                        )],
                        (10, 5),
                    ),
                    answering("never sent", (1, 1)),
                ],
                CancellationToken::default(),
                |_| {},
            );
            // The response is refused whole, before any call is admitted.
            let receipt = looped.settled.expect("settled");
            assert_eq!(receipt.result, None);
            assert!(job_record(&handle, run.job).unknown_at.is_some());
            assert!(
                evidence(&handle, run.job).is_empty(),
                "nothing was admitted"
            );

            // A program that is not found is refused before anything starts.
            let run = local_run(&handle, llm);
            let (_empty_guard, empty) =
                crate::test_support::temporary_directory(SUITE, "command-refused-empty");
            let path = empty.display().to_string();
            let tools = workspace_command::code_tools_with(
                Workspace::authorize(&project.root, &directory).expect("authorize"),
                CommandEnvironment::from_source(move |name| {
                    (name == "PATH").then(|| path.clone().into())
                }),
                crate::test_support::WAIT,
            )
            .expect("enable the tools");
            let looped = run_loop(
                &handle,
                run,
                Some(tools),
                vec![
                    calling(&[test_call(0, "call_test")], (10, 5)),
                    answering("never sent", (1, 1)),
                ],
                CancellationToken::default(),
                |_| {},
            );
            let (_, stop) = stopped(looped);
            let message = "the command's program was not found on OCG's PATH".to_owned();
            assert_eq!(
                stop,
                ToolLoopStop::HandlerFailed {
                    index: 0,
                    message: message.clone()
                }
            );
            let [record] = evidence(&handle, run.job).try_into().expect("one call");
            assert_eq!(
                outcome_of(&record),
                Some(ToolOutcome::NotApplied { message })
            );
            assert!(!record.is_applied() && !record.is_unresolved());
            assert!(job_record(&handle, run.job).failed_at.is_some());
            wait(handle.retry_job(retry_of(run.job, llm, false))).expect("nothing ran");
            assert_eq!(project.runs(), 0);
            assert!(!project.root.join("ran").exists());
            worker.shutdown().expect("shutdown");
        }

        #[test]
        fn a_command_whose_outcome_cannot_be_recorded_is_never_reported_as_done() {
            let project = setup("command-unrecorded", TESTING);
            let (_root, directory) = data_directory("command-unrecorded");
            let worker = DatabaseWorker::start_with_config(&directory, lenient()).expect("start");
            let handle = worker.handle();
            let (_provider, llm) = llm_setup(&handle, "model-a");
            let run = local_run(&handle, llm);
            let (seam_handle, path) = (handle.clone(), directory.database_path().to_path_buf());
            dispatch_seam::install(move |point, _| {
                if point == Point::Ran {
                    protect(&seam_handle, &path);
                }
            });
            let looped = run_loop(
                &handle,
                run,
                Some(project.tools(&directory, crate::test_support::WAIT)),
                vec![
                    calling(&[test_call(0, "call_test")], (10, 5)),
                    answering("never sent", (1, 1)),
                ],
                CancellationToken::default(),
                |_| {},
            );
            assert_eq!(looped.sent.len(), 1, "an unrecorded result is never sent");
            match looped.settled {
                Err(ExecutionRuntimeError::ToolLoopNotSettled { stop, .. }) => assert_eq!(
                    *stop,
                    ToolLoopStop::EvidenceNotRecorded {
                        index: 0,
                        outcome: Some(ToolOutcomeKind::Completed),
                    }
                ),
                other => panic!("the unrecorded outcome must be reported, got {other:?}"),
            }
            assert_eq!(project.runs(), 1);
            worker.shutdown().expect("shutdown");

            let worker = DatabaseWorker::start_with_config(&directory, lenient()).expect("restart");
            let handle = worker.handle();
            assert_eq!(handle.abandoned_at_open(), [run.job]);
            let job = job_record(&handle, run.job);
            assert!(job.unknown_at.is_some() && job.completed_at.is_none());
            assert!(job.stop_confirmed_at.is_none());
            let [record] = evidence(&handle, run.job).try_into().expect("one call");
            assert!(record.outcome.is_none() && record.is_unresolved());
            assert_eq!(
                violation(wait(handle.retry_job(retry_of(run.job, llm, true)))),
                Violation::RetryOfUnsettledJob(run.job)
            );
            assert_eq!(project.runs(), 1, "nothing ran again");
            worker.shutdown().expect("shutdown");
            assert_relationships_consistent(&directory);
        }

        #[test]
        fn a_crash_after_a_command_ran_leaves_it_unresolved_and_a_restart_replays_nothing() {
            let project = setup("command-crash", TESTING);
            let (_root, directory) = data_directory("command-crash");
            let worker = DatabaseWorker::start(&directory).expect("start database");
            let handle = worker.handle();
            let (_provider, llm) = llm_setup(&handle, "model-a");
            let run = local_run(&handle, llm);
            let tools = project.tools(&directory, crate::test_support::WAIT);
            let loop_handle = handle.clone();
            let died = std::thread::spawn(move || {
                dispatch_seam::install(|point, _| {
                    assert_ne!(point, Point::Ran, "the process died here");
                });
                run_loop(
                    &loop_handle,
                    run,
                    Some(tools),
                    vec![
                        calling(&[test_call(0, "call_test")], (10, 5)),
                        answering("never sent", (1, 1)),
                    ],
                    CancellationToken::default(),
                    |_| {},
                );
            })
            .join();
            assert!(died.is_err(), "the process died after the command ran");
            assert_eq!(project.runs(), 1);
            worker.shutdown().expect("shutdown");

            for _ in 0..2 {
                let worker = DatabaseWorker::start(&directory).expect("restart");
                let handle = worker.handle();
                let job = job_record(&handle, run.job);
                assert!(job.unknown_at.is_some() && job.stop_confirmed_at.is_none());
                assert_eq!(blocking(&handle, llm), [run.job]);
                let [record] = evidence(&handle, run.job).try_into().expect("one call");
                assert!(
                    record.admission().is_some()
                        && record.outcome.is_none()
                        && record.is_unresolved()
                );
                assert_eq!(
                    violation(wait(handle.retry_job(retry_of(run.job, llm, true)))),
                    Violation::RetryOfUnsettledJob(run.job)
                );
                worker.shutdown().expect("shutdown");
                assert_eq!(project.runs(), 1, "no restart runs it again");
            }
            assert_eq!(project.source().as_deref(), Some(BUGGY));
            assert_relationships_consistent(&directory);
        }

        #[test]
        fn a_group_whose_proof_failed_is_proven_and_released_on_retry() {
            use std::sync::atomic::{AtomicBool, Ordering};
            use crate::orchestration::blocking::{self, Kind, Process, StopSignal};

            let (_root, directory) = data_directory("unproven-group-retry");
            let policy =
                crate::system_policy::SystemPolicy::load_for_startup(&directory).expect("policy");
            let worker =
                DatabaseWorker::start_with_policy(&directory, policy).expect("start database");
            let handle = worker.handle();
            let (_provider, llm) = llm_setup(&handle, "model-a");
            let run = local_run(&handle, llm);
            let job = run.job;

            let request = RequestNumber::FIRST;
            wait(handle.record_metrics(NewMetrics {
                job,
                request,
                observed_at: Timestamp::now(),
                measurements: Measurements {
                    ttfb_ms: Some(1),
                    ..Measurements::default()
                },
                token: None,
            }))
            .unwrap();
            let call = ToolCallRef {
                job,
                request,
                index: 0,
            };
            let admission = ToolAdmission {
                handler_version: "workspace_run_command-4".into(),
                effect: ToolEffect::NonIdempotent,
                parameters: "{}".into(),
                arguments: ToolPayload::new("{}"),
                authorization: AuthorizationBasis::RegistryGrant,
            };
            wait(handle.record_tool_decision(NewToolEvidence {
                call,
                tool_call_id: "call-1".into(),
                name: "workspace_run_command".into(),
                spawns: true,
                decision: ToolDecision::Admitted(admission),
            }))
            .unwrap();

            wait(handle.finalize_job(job, JobFinalization::Unknown)).expect("finalize unknown");
            let unresolved = wait(handle.unresolved_executions()).expect("unresolved");
            assert_eq!(unresolved.len(), 1);
            assert_eq!(unresolved[0].0, job);

            let mut cmd = std::process::Command::new("sleep");
            cmd.arg("10");
            let stop = StopSignal::new();
            let process = Process::spawn(cmd, Kind::Command, stop.clone(), Some(job))
                .expect("spawn process");
            let target_pid = process.group();

            let should_succeed = Arc::new(AtomicBool::new(false));
            let should_succeed_clone = should_succeed.clone();
            blocking::set_proof_hook(Some(Box::new(move |pid, _sentinel| {
                if pid == target_pid {
                    if should_succeed_clone.load(Ordering::SeqCst) {
                        Some(Ok(()))
                    } else {
                        Some(Err(std::io::Error::new(
                            std::io::ErrorKind::TimedOut,
                            "simulated proof failure",
                        )))
                    }
                } else {
                    None
                }
            })));

            stop.request();
            let finish_err = process.finish(|| true);
            assert!(finish_err.is_err());

            let released = blocking::retry_unproven_groups();
            assert!(released.is_empty());
            assert_eq!(wait(handle.unresolved_executions()).unwrap().len(), 1);

            should_succeed.store(true, Ordering::SeqCst);
            let released = blocking::retry_unproven_groups();
            assert_eq!(released, vec![job]);

            for job in released {
                wait(handle.confirm_job_stopped(job)).expect("confirm stop");
            }
            assert!(wait(handle.unresolved_executions()).unwrap().is_empty());

            blocking::set_proof_hook(None);
            blocking::drop_unproven_groups();
            worker.shutdown().expect("shutdown");

            let unquarantined_err = crate::release_slot_cli::run(
                &directory,
                vec!["--yes".to_string(), job.to_string()],
            );
            assert!(unquarantined_err.is_err());
            assert!(unquarantined_err.unwrap_err().to_string().contains("is not quarantined"));
        }
    }

    // ---- Workspace discovery and ranged reading -----------------------------

    #[cfg(any(target_os = "linux", target_os = "android", target_vendor = "apple"))]
    mod workspace_discovery {
        use std::path::{Path, PathBuf};

        use super::*;
        use crate::orchestration::workspace_command;
        use crate::orchestration::workspace_discovery::{LIST_TOOL_NAME, SEARCH_TOOL_NAME};
        use crate::orchestration::workspace_read::{self, Workspace};

        /// The line of `src/net/client.rs` that holds the answer.
        const ANSWER_LINE: usize = 297;

        /// A project whose answer lies in a file over the whole-file limit,
        /// with decoys a search must never show.
        fn project(name: &str) -> (TestRoot, PathBuf, String) {
            let (guard, root) =
                crate::test_support::temporary_directory(SUITE, &format!("{name}-files"));
            let write = |path: &str, content: &str| {
                let path = root.join(path);
                std::fs::create_dir_all(path.parent().expect("a parent")).expect("create");
                std::fs::write(path, content).expect("write");
            };
            write("Cargo.toml", "[package]\nname = \"demo\"\n");
            write("README.md", "# Demo\n\nA small networking demo.\n");
            write("src/lib.rs", "pub mod net;\n");
            write("src/net/mod.rs", "pub mod client;\npub mod server;\n");
            write("src/net/server.rs", "pub fn serve() {}\n");
            let client = (1..=600)
                .map(|line| match line {
                    295 => "/// How long the client waits for a connection, in milliseconds.\n"
                        .to_owned(),
                    296 => "/// Chosen to stay under the gateway's 3 s limit.\n".to_owned(),
                    ANSWER_LINE => "pub const CONNECT_TIMEOUT_MS: u64 = 2_750;\n".to_owned(),
                    _ => format!("// client implementation detail {line:03}\n"),
                })
                .collect::<String>();
            assert!(client.len() > workspace_read::MAX_FILE_BYTES);
            write("src/net/client.rs", &client);
            write(".env", "CONNECT_TIMEOUT_MS=1\n");
            write(
                "target/debug/build.rs",
                "const CONNECT_TIMEOUT_MS: u64 = 9;\n",
            );
            write("config/secrets.yaml", "CONNECT_TIMEOUT_MS: 5\n");
            (guard, root, client)
        }

        fn parsed(text: &str) -> Value {
            serde_json::from_str(text).expect("JSON")
        }

        /// The result of the last tool call a conversation answered.
        fn last_result(conversation: &ChatConversation) -> Value {
            let text = conversation
                .messages
                .iter()
                .rev()
                .find_map(|message| match (&message.tool_call_id, &message.content) {
                    (Some(_), Some(MessageContentWire::Text(text))) => Some(text.clone()),
                    _ => None,
                })
                .expect("a tool result");
            parsed(&text)
        }

        /// A Provider that knows no file name: it lists the root, searches
        /// the source directory it finds for what the objective names,
        /// reads the lines around the first match, and answers from them.
        /// Each step is derived from the results OCG sent it.
        fn agent(conversation: &ChatConversation) -> Vec<Step> {
            let tool = |id: &str, name: &str, arguments: Value| {
                calling(&[call(0, id, name, &arguments.to_string())], (40, 10))
            };
            let requests = conversation
                .messages
                .iter()
                .filter(|message| message.tool_calls.is_some())
                .count();
            match requests {
                0 => tool("call_list", LIST_TOOL_NAME, json!({})),
                1 => {
                    let listing = last_result(conversation);
                    let source = listing["entries"]
                        .as_array()
                        .expect("entries")
                        .iter()
                        .find(|entry| entry["type"] == "directory" && entry["name"] == "src")
                        .expect("a source directory is listed");
                    tool(
                        "call_search",
                        SEARCH_TOOL_NAME,
                        json!({"path": source["name"], "text": "CONNECT_TIMEOUT_MS"}),
                    )
                }
                2 => {
                    let found = last_result(conversation);
                    let file = &found["files"][0];
                    let line = file["matches"][0]["line"].as_u64().expect("a line");
                    // Too large to read whole, so read the lines around it.
                    assert!(file["bytes"].as_u64().expect("a size") > 8 * 1024);
                    tool(
                        "call_read",
                        workspace_read::TOOL_NAME,
                        json!({"path": file["path"], "start_line": line - 2, "line_count": 3}),
                    )
                }
                _ => {
                    let read = last_result(conversation);
                    let content = read["content"].as_str().expect("content");
                    let value = content
                        .lines()
                        .find_map(|line| line.strip_prefix("pub const CONNECT_TIMEOUT_MS: u64 = "))
                        .and_then(|rest| rest.strip_suffix(';'))
                        .expect("the constant is in the lines read");
                    let why = content
                        .lines()
                        .find_map(|line| line.strip_prefix("/// Chosen to "))
                        .expect("its reason is in the lines read");
                    answering(
                        &format!(
                            "The client's connect timeout is {value} ms ({}:{}), chosen to {why}",
                            read["path"].as_str().expect("a path"),
                            read["end_line"],
                        ),
                        (80, 20),
                    )
                }
            }
        }

        /// Runs the real tool loop on `run` with `objective`, answering each
        /// request with what `respond` derives from the conversation sent.
        fn run_agent(
            handle: &DatabaseHandle,
            run: TokenRun,
            tools: ToolExecution,
            objective: &str,
            mut respond: impl FnMut(&ChatConversation) -> Vec<Step> + 'static,
        ) -> Looped {
            let handle = handle.clone();
            let objective = objective.to_owned();
            on_ntex(async move {
                let cancellation = CancellationToken::default();
                let config = ProviderExecutionConfig::default();
                let conversation = ChatConversation {
                    messages: vec![ChatMessage::user(objective)],
                    tools: tools.offer.wires().expect("offer"),
                    ..ChatConversation::default()
                };
                let mut sent = Vec::new();
                let settled = run_provider_job(
                    ProviderRun {
                        database: handle,
                        executor: run.executor,
                        input: run.input,
                        job: run.job,
                    },
                    conversation,
                    Some(&tools),
                    &cancellation,
                    async |conversation: &ChatConversation, sink: &mut DatabaseOutputSink| {
                        sent.push(conversation.clone());
                        conversation
                            .validate()
                            .map_err(|error| ProviderExecutionError::Build {
                                message: error.to_string(),
                            })?;
                        let steps = respond(conversation);
                        announce(Some(&mut *sink), ProviderFact::Delivered).await;
                        read_response(
                            payload(steps, config.cancellation.clone()),
                            200,
                            Instant::now(),
                            None,
                            &config,
                            Some(&mut *sink),
                            conversation,
                        )
                        .await
                    },
                )
                .await;
                Looped { settled, sent }
            })
        }

        fn authorize(root: &Path, directory: &DataDirectory) -> Workspace {
            Workspace::authorize(root, directory).expect("authorize")
        }

        #[test]
        fn indexed_search_records_the_real_handler_result_as_tool_evidence() {
            use crate::orchestration::workspace_fts;
            let (_files, root, _) = project("indexed-tool-evidence");
            let (_root, directory) = data_directory("indexed-tool-evidence");
            let worker = DatabaseWorker::start(&directory).expect("database");
            let handle = worker.handle();
            let (_provider, llm) = llm_setup(&handle, "model-a");
            let run = local_run(&handle, llm);
            let tools = workspace_command::code_tools_with(
                authorize(&root, &directory),
                workspace_command::CommandEnvironment::of_process(),
                std::time::Duration::from_secs(10),
            )
            .expect("default indexed coding tools");
            let looped = run_agent(&handle, run, tools, "Find the timeout", |conversation| {
                if conversation
                    .messages
                    .iter()
                    .any(|message| message.tool_calls.is_some())
                {
                    let result = last_result(conversation);
                    assert_eq!(result["index"]["external_modifications"], "unverified");
                    assert!(!result["results"].as_array().expect("results").is_empty());
                    answering("found the indexed timeout", (20, 5))
                } else {
                    calling(
                        &[call(
                            0,
                            "call_fts",
                            workspace_fts::TOOL_NAME,
                            &json!({"query":"CONNECT_TIMEOUT_MS"}).to_string(),
                        )],
                        (20, 5),
                    )
                }
            });
            looped.settled.expect("settled");
            let [record] = evidence(&handle, run.job).try_into().expect("one call");
            assert_eq!(record.name, workspace_fts::TOOL_NAME);
            let admission = record.admission().expect("admitted");
            assert_eq!(admission.handler_version, "workspace_fts_search-2");
            assert_eq!(admission.effect, ToolEffect::ReadOnly);
            assert_eq!(admission.authorization, AuthorizationBasis::RegistryGrant);
            let ToolOutcome::Completed { result } = outcome_of(&record).expect("outcome") else {
                panic!("completed search");
            };
            assert_eq!(parsed(result.as_str()), last_result(&looped.sent[1]));
            assert!(!record.is_unresolved());
            worker.shutdown().expect("shutdown");
            assert_relationships_consistent(&directory);
        }

        #[test]
        fn a_coding_agent_discovers_searches_and_reads_a_file_it_was_never_named() {
            let (_files, root, client) = project("discovery-agent");
            let (_root, directory) = data_directory("discovery-agent");
            let worker = DatabaseWorker::start(&directory).expect("start database");
            let handle = worker.handle();
            let (_provider, llm) = llm_setup(&handle, "model-a");
            let run = local_run(&handle, llm);
            // The registry `ocg workspace-code` builds.
            let tools = workspace_command::code_tools(authorize(&root, &directory))
                .expect("enable the coding tools");
            let looped = run_agent(
                &handle,
                run,
                tools,
                "What connect timeout does the networking client use, and why?",
                agent,
            );
            let receipt = looped.settled.expect("the loop settles");
            assert_eq!(receipt.job, run.job);
            let [first, second, third, fourth] = looped.sent.as_slice() else {
                panic!("four Provider requests: {}", looped.sent.len());
            };
            let offered = first
                .tools
                .iter()
                .map(|tool| tool.function.name.as_str())
                .collect::<Vec<_>>();
            assert!(
                offered.contains(&LIST_TOOL_NAME)
                    && offered.contains(&SEARCH_TOOL_NAME)
                    && offered.contains(&workspace_read::TOOL_NAME),
                "{offered:?}"
            );

            // What OCG sent back at each step, from the real files.
            let listing = last_result(second);
            assert_eq!(
                listing["entries"],
                json!([
                    {"name": "Cargo.toml", "type": "file", "bytes": 24},
                    {"name": "README.md", "type": "file", "bytes": 33},
                    {"name": "config", "type": "directory"},
                    {"name": "src", "type": "directory"},
                    {"name": "target", "type": "directory"},
                ]),
                "hidden names are never listed"
            );
            let found = last_result(third);
            assert_eq!(
                found,
                json!({
                    "path": "src",
                    "files": [{
                        "path": "src/net/client.rs",
                        "bytes": client.len(),
                        "matches": [{
                            "line": ANSWER_LINE,
                            "text": "pub const CONNECT_TIMEOUT_MS: u64 = 2_750;",
                        }],
                        "more_matches": false,
                    }],
                    "complete": true,
                    "limit": null,
                    "skipped": 0,
                })
            );
            let read = last_result(fourth);
            let expected = client
                .lines()
                .skip(ANSWER_LINE - 3)
                .take(3)
                .map(|line| format!("{line}\n"))
                .collect::<String>();
            assert_eq!(
                read,
                json!({
                    "path": "src/net/client.rs",
                    "start_line": ANSWER_LINE - 2,
                    "end_line": ANSWER_LINE,
                    "total_lines": 600,
                    "next_start_line": ANSWER_LINE + 1,
                    "line_truncated": false,
                    "content": expected,
                }),
                "exactly the lines asked for"
            );

            // The answer is grounded in the lines read.
            let job = job_record(&handle, run.job);
            assert!(job.completed_at.is_some());
            let outputs = wait(handle.job_outputs(run.job, None, 100)).expect("outputs");
            assert_eq!(
                outputs.last().and_then(|output| output.summary.as_deref()),
                Some(
                    "The client's connect timeout is 2_750 ms (src/net/client.rs:297), \
                     chosen to stay under the gateway's 3 s limit."
                )
            );

            // Every call is recorded as a read-only admission with the result sent.
            let records = evidence(&handle, run.job);
            let [list, search, range] = records.as_slice() else {
                panic!("three calls: {records:?}");
            };
            for (record, request, name, version, sent) in [
                (
                    list,
                    1,
                    LIST_TOOL_NAME,
                    "workspace_list_directory-1",
                    second,
                ),
                (search, 2, SEARCH_TOOL_NAME, "workspace_search-1", third),
                (
                    range,
                    3,
                    workspace_read::TOOL_NAME,
                    "workspace_read_file-2",
                    fourth,
                ),
            ] {
                assert_eq!(record.call, tool_call(run.job, request, 0));
                assert_eq!(record.name, name);
                let admission = record.admission().expect("admitted");
                assert_eq!(admission.handler_version, version);
                assert_eq!(admission.effect, ToolEffect::ReadOnly);
                assert_eq!(admission.authorization, AuthorizationBasis::RegistryGrant);
                let ToolOutcome::Completed { result } = outcome_of(record).expect("an outcome")
                else {
                    panic!("completed: {record:?}");
                };
                assert_eq!(parsed(result.as_str()), last_result(sent));
                assert!(!record.is_unresolved());
            }
            for record in &records {
                let recorded = format!("{record:?}");
                for hidden in ["CONNECT_TIMEOUT_MS=1", ".env", "build.rs", "secrets"] {
                    assert!(!recorded.contains(hidden), "{hidden} was never disclosed");
                }
            }
            // Read-only calls repeat no effect, so a retry needs no acknowledgement.
            wait(handle.retry_job(retry_of(run.job, llm, false))).expect("a retry");
            worker.shutdown().expect("shutdown");
            assert_relationships_consistent(&directory);
        }

        #[test]
        fn a_refused_discovery_call_sends_nothing_and_fails_the_job() {
            let (_files, root, _) = project("discovery-refused");
            let (_outside_guard, outside) =
                crate::test_support::temporary_directory(SUITE, "discovery-refused-outside");
            std::fs::write(
                outside.join("secret.rs"),
                "const CONNECT_TIMEOUT_MS: u64 = 1;\n",
            )
            .expect("write");
            std::os::unix::fs::symlink(&outside, root.join("vendor")).expect("link");
            let (_root, directory) = data_directory("discovery-refused");
            let worker = DatabaseWorker::start(&directory).expect("start database");
            let handle = worker.handle();
            let (_provider, llm) = llm_setup(&handle, "model-a");
            let run = local_run(&handle, llm);
            let tools = workspace_command::code_tools(authorize(&root, &directory))
                .expect("enable the coding tools");
            let looped = run_agent(&handle, run, tools, "Search vendor.", |_| {
                calling(
                    &[call(
                        0,
                        "call_search",
                        SEARCH_TOOL_NAME,
                        r#"{"path":"vendor","text":"CONNECT_TIMEOUT_MS"}"#,
                    )],
                    (10, 5),
                )
            });
            assert_eq!(looped.sent.len(), 1, "a refused call sends no result");
            let (_, stop) = stopped(looped);
            let message = "the path traverses a symbolic link".to_owned();
            assert_eq!(
                stop,
                ToolLoopStop::HandlerFailed {
                    index: 0,
                    message: message.clone()
                }
            );
            let [record] = evidence(&handle, run.job).try_into().expect("one call");
            assert_eq!(
                outcome_of(&record),
                Some(ToolOutcome::NotApplied { message })
            );
            assert!(job_record(&handle, run.job).failed_at.is_some());
            worker.shutdown().expect("shutdown");
        }
    }

    // ---- Native Job admission ---------------------------------------------------

    mod native_admission {
        use super::*;
        use crate::database::CommandOutcome;
        use crate::orchestration::execution_runtime::provider_seam::{self, Response};
        use crate::orchestration::execution_runtime::{AdmissionError, AdmittedJob};
        use crate::provider_execution::EnvironmentCredentialResolver;
        use crate::provider_execution::scripted::Step;

        /// A Provider answered by the test, its LLM Profile and an Executor.
        struct Target {
            executor: ExecutorId,
            llm: ProfileId,
        }

        /// Registers a Provider named `name`, whose endpoint is unique to the
        /// test, and answers every request to it with the next of
        /// `responses`. Returns the target and the count of requests.
        fn scripted(
            handle: &DatabaseHandle,
            name: &str,
            responses: Vec<Response>,
        ) -> (Target, Arc<AtomicUsize>) {
            let provider = provider(handle, name);
            let llm = llm_profile(
                handle,
                provider,
                llm_config(name, &[ModelCapability::Chat, ModelCapability::Tools]),
            );
            let executor = executor(handle, "llm worker", &[OPENAI]).expect("register");
            let requests = Arc::new(AtomicUsize::new(0));
            let counted = requests.clone();
            let mut responses = responses.into_iter();
            provider_seam::install(&format!("https://{name}.invalid/v1"), move |_| {
                counted.fetch_add(1, Ordering::SeqCst);
                responses
                    .next()
                    .expect("a scripted response for every request")
            });
            (Target { executor, llm }, requests)
        }

        fn plan(
            handle: &DatabaseHandle,
            target: &Target,
            name: &str,
            tools: Option<ToolExecution>,
        ) -> ProviderExecutionPlan {
            ProviderExecutionPlan {
                executor_id: target.executor,
                input: JobInputDraft {
                    objective: Some(format!("the objective of {name}")),
                    ..JobInputDraft::default()
                },
                job: NewJob {
                    configuration: llm_only(target.llm),
                    ..new_job(handle, name, handle.genesis(), Vec::new())
                },
                credential_resolver: Arc::new(EnvironmentCredentialResolver),
                provider_config: ProviderExecutionConfig::default(),
                tools,
            }
        }

        fn now(steps: Vec<Step>) -> Response {
            Response { gate: None, steps }
        }

        /// A response read only once the returned sender sends or drops.
        fn gated(steps: Vec<Step>) -> (flume::Sender<()>, Response) {
            let (open, gate) = flume::bounded(1);
            (
                open,
                Response {
                    gate: Some(gate),
                    steps,
                },
            )
        }

        /// A response that never comes: only cancellation ends it.
        fn stalled() -> Response {
            now(vec![Step::Stall])
        }

        fn jobs_named(handle: &DatabaseHandle, name: &str) -> Vec<JobRecord> {
            wait(handle.jobs(None, 1_000))
                .expect("read Jobs")
                .into_iter()
                .filter(|job| job.name.as_deref() == Some(name))
                .collect()
        }

        fn admitted(result: Result<AdmittedJob, AdmissionError>) -> AdmittedJob {
            result.unwrap_or_else(|error| panic!("the plan must be admitted, got {error}"))
        }

        fn settled(record: &JobRecord) -> bool {
            record.completed_at.is_some()
                || record.failed_at.is_some()
                || record.cancelled_at.is_some()
                || record.unknown_at.is_some()
        }

        fn until(mut condition: impl FnMut() -> bool) {
            let deadline = std::time::Instant::now() + crate::test_support::WAIT;
            while !condition() {
                assert!(std::time::Instant::now() < deadline, "not reached in time");
                std::thread::sleep(Duration::from_millis(5));
            }
        }

        fn summary(handle: &DatabaseHandle, job: JobId) -> Option<String> {
            wait(handle.job_outputs(job, None, 100))
                .expect("outputs")
                .into_iter()
                .rev()
                .find(|output| output.kind == OutputKind::Result)
                .and_then(|output| output.summary)
        }

        #[test]
        fn dispatcher_enforces_pool_and_global_limits_and_wakes_on_release() {
            let (_root, directory) = data_directory("dispatcher-concurrency");
            let worker = DatabaseWorker::start(&directory).expect("database");
            let handle = worker.handle();
            let (target, requests) = scripted(
                &handle,
                "dispatcher-concurrency",
                (0..20).map(|_| stalled()).collect(),
            );
            let pool = crate::persistence::ExecutionPoolId::from(target.executor);
            let mut config = wait(handle.execution_pool(pool))
                .expect("read")
                .expect("pool")
                .config;
            config.concurrency_limit = NonZeroU32::new(2).expect("two slots");
            wait(handle.update_execution_pool(pool, config.clone())).expect("two slots");
            let runtime = ExecutionRuntime::start(handle.clone()).expect("runtime");
            let mut jobs = Vec::new();
            for index in 0..20 {
                jobs.push(admitted(wait(runtime.scheduler().admit_provider(plan(
                    &handle,
                    &target,
                    &format!("parallel {index}"),
                    None,
                )))));
            }
            until(|| requests.load(Ordering::SeqCst) == 2);
            assert_eq!(wait(handle.occupancy()).expect("slots").len(), 2);
            assert!(job_record(&handle, jobs[19].job()).started_at.is_none());
            config.concurrency_limit = NonZeroU32::new(32).expect("wide pool");
            wait(handle.update_execution_pool(pool, config)).expect("capacity wake");
            until(|| requests.load(Ordering::SeqCst) == 16);
            assert_eq!(wait(handle.occupancy()).expect("slots").len(), 16);
            let pending = jobs.pop().expect("pending Job");
            let pending_id = pending.job();
            pending.request_stop();
            wait(pending.settled()).expect("pending cancellation");
            let record = job_record(&handle, pending_id);
            assert!(record.started_at.is_none() && record.cancelled_at.is_some());
            jobs[0].request_stop();
            until(|| requests.load(Ordering::SeqCst) == 17);
            assert_eq!(wait(handle.occupancy()).expect("slots").len(), 16);
            wait(runtime.shutdown()).expect("shutdown every task");
            for job in jobs {
                wait(job.settled()).expect("settled on shutdown");
            }
            assert!(wait(handle.occupancy()).expect("slots").is_empty());
            worker.shutdown().expect("shutdown database");
        }

        #[test]
        fn dispatcher_finalizes_a_panicking_job_and_keeps_dispatching() {
            let (_root, directory) = data_directory("dispatcher-panic");
            let worker = DatabaseWorker::start(&directory).expect("database");
            let handle = worker.handle();
            let (target, _) = scripted(&handle, "dispatcher-panic", Vec::new());
            let mut panic_once = true;
            provider_seam::install("https://dispatcher-panic.invalid/v1", move |_| {
                if std::mem::take(&mut panic_once) {
                    panic!("injected Job task panic");
                }
                now(answering("dispatcher continued", (2, 1)))
            });
            let runtime = ExecutionRuntime::start(handle.clone()).expect("runtime");
            let first = admitted(wait(
                runtime
                    .scheduler()
                    .admit_provider(plan(&handle, &target, "panics", None)),
            ));
            let id = first.job();
            assert!(matches!(
                wait(first.settled()),
                Err(ExecutionRuntimeError::WorkerPanicked)
            ));
            assert!(job_record(&handle, id).unknown_at.is_some());
            assert!(wait(handle.occupancy()).expect("capacity").is_empty());
            let second = admitted(wait(runtime.scheduler().admit_provider(plan(
                &handle,
                &target,
                "continues",
                None,
            ))));
            let id = second.job();
            wait(second.settled()).expect("still dispatching");
            assert!(job_record(&handle, id).completed_at.is_some());
            wait(runtime.shutdown()).expect("shutdown");
            worker.shutdown().expect("shutdown database");
        }

        #[test]
        fn dispatcher_admission_commits_before_capacity_and_preserves_submission_identity() {
            use crate::persistence::{NewSubmission, RequestFingerprint, SubmissionKey};
            let (_root, directory) = data_directory("dispatcher-admission");
            let worker = DatabaseWorker::start(&directory).expect("database");
            let handle = worker.handle();
            let (target, requests) = scripted(
                &handle,
                "dispatcher-admission",
                vec![now(answering("done", (2, 1)))],
            );
            let pool = crate::persistence::ExecutionPoolId::from(target.executor);
            wait(handle.set_execution_pool_enabled(pool, false)).expect("disable");
            let runtime = ExecutionRuntime::start(handle.clone()).expect("runtime");
            let submission = NewSubmission {
                key: SubmissionKey::from_digest([42; 32]),
                fingerprint: RequestFingerprint::from_digest([17; 32]),
            };
            let job = admitted(wait(runtime.scheduler().admit_submitted_provider(
                plan(&handle, &target, "pending admission", None),
                submission,
            )));
            let record = job_record(&handle, job.job());
            assert!(record.started_at.is_none() && record.claim.is_none());
            assert_eq!(
                wait(handle.submission(submission.key))
                    .expect("submission")
                    .expect("committed")
                    .job,
                job.job()
            );
            assert!(matches!(
                wait(runtime.scheduler().admit_submitted_provider(
                    plan(&handle, &target, "duplicate", None),
                    submission
                )),
                Err(AdmissionError::NotAdmitted(
                    ExecutionRuntimeError::Database(DatabaseError::Rejected(
                        Violation::SubmissionKeyUsed { .. }
                    ))
                ))
            ));
            let receipt = wait(runtime.scheduler().schedule(ExecutionPlan {
                executor_id: target.executor,
                input: JobInputDraft::default(),
                job: plan(&handle, &target, "plain pending", None).job,
                outcome: JobOutcome::Completed,
                result: result("plain done"),
            }))
            .expect("pending plain Job");
            assert!(job_record(&handle, receipt.job).claim.is_none());
            assert_eq!(requests.load(Ordering::SeqCst), 0);
            wait(handle.set_execution_pool_enabled(pool, true)).expect("pool wake");
            wait(job.settled()).expect("provider completion");
            until(|| job_record(&handle, receipt.job).completed_at.is_some());
            assert_eq!(requests.load(Ordering::SeqCst), 1);
            wait(runtime.shutdown()).expect("shutdown runtime");
            worker.shutdown().expect("shutdown database");
        }

        #[test]
        fn dispatcher_promotes_delays_and_wakes_dependency_waiters() {
            let (_root, directory) = data_directory("dispatcher-readiness");
            let worker = DatabaseWorker::start(&directory).expect("database");
            let handle = worker.handle();
            let (open, response) = gated(answering("first", (2, 1)));
            let (target, requests) = scripted(
                &handle,
                "dispatcher-readiness",
                vec![
                    response,
                    now(answering("second", (2, 1))),
                    now(answering("delayed", (2, 1))),
                ],
            );
            let runtime = ExecutionRuntime::start(handle.clone()).expect("runtime");
            let first = admitted(wait(runtime.scheduler().admit_provider(plan(
                &handle,
                &target,
                "prerequisite",
                None,
            ))));
            let mut second = plan(&handle, &target, "waiter", None);
            second.job.dependencies = vec![(first.job(), DependencyCondition::Success)];
            let second = admitted(wait(runtime.scheduler().admit_provider(second)));
            until(|| requests.load(Ordering::SeqCst) == 1);
            assert!(job_record(&handle, second.job()).claim.is_none());
            open.send(()).expect("complete prerequisite");
            wait(first.settled()).expect("first");
            wait(second.settled()).expect("dependency wake");
            let mut delayed = plan(&handle, &target, "delayed", None);
            let began = Instant::now();
            delayed.job.eligible_at = Some(Timestamp::from_unix_micros(
                Timestamp::now().as_unix_micros() + 200_000,
            ));
            let delayed = admitted(wait(runtime.scheduler().admit_provider(delayed)));
            assert!(job_record(&handle, delayed.job()).claim.is_none());
            wait(delayed.settled()).expect("timer promotion");
            assert!(began.elapsed() >= Duration::from_millis(180));
            assert_eq!(requests.load(Ordering::SeqCst), 3);
            wait(runtime.shutdown()).expect("shutdown");
            worker.shutdown().expect("shutdown");
        }

        #[test]
        fn dispatcher_reports_dependency_cancellation_without_executing_the_waiter() {
            let (_root, directory) = data_directory("dispatcher-impossible");
            let worker = DatabaseWorker::start(&directory).expect("database");
            let handle = worker.handle();
            let (target, requests) = scripted(&handle, "dispatcher-impossible", vec![stalled()]);
            let runtime = ExecutionRuntime::start(handle.clone()).expect("runtime");
            let first = admitted(wait(
                runtime
                    .scheduler()
                    .admit_provider(plan(&handle, &target, "first", None)),
            ));
            let mut waiter = plan(&handle, &target, "waiter", None);
            waiter.job.dependencies = vec![(first.job(), DependencyCondition::Success)];
            let waiter = admitted(wait(runtime.scheduler().admit_provider(waiter)));
            until(|| requests.load(Ordering::SeqCst) == 1);
            first.request_stop();
            wait(first.settled()).expect("first stopped");
            let id = waiter.job();
            let mut settlement = std::pin::pin!(waiter.settled());
            let deadline = Instant::now() + Duration::from_secs(35);
            loop {
                if let Some(result) = poll_once(settlement.as_mut()) {
                    result.expect("cancelled by dependency, found on the reconcile tick");
                    break;
                }
                assert!(
                    Instant::now() < deadline,
                    "reconciliation must answer the waiter"
                );
                std::thread::sleep(Duration::from_millis(10));
            }
            assert!(job_record(&handle, id).cancelled_at.is_some());
            assert!(job_record(&handle, id).claim.is_none());
            assert_eq!(requests.load(Ordering::SeqCst), 1);
            wait(runtime.shutdown()).expect("shutdown");
            worker.shutdown().expect("shutdown");
        }

        #[test]
        fn dispatcher_cancels_a_durable_pending_job_after_reopening() {
            let (_root, directory) = data_directory("dispatcher-reopen");
            let worker = DatabaseWorker::start(&directory).expect("database");
            let handle = worker.handle();
            let (target, requests) = scripted(
                &handle,
                "dispatcher-reopen",
                vec![now(answering("recovered", (2, 1)))],
            );
            let job = wait(handle.create_job(plan(&handle, &target, "stored pending", None).job))
                .expect("T1");
            worker.shutdown().expect("shutdown");
            let worker = DatabaseWorker::start(&directory).expect("reopen");
            let handle = worker.handle();
            let runtime = ExecutionRuntime::start(handle.clone()).expect("runtime");
            let record = job_record(&handle, job);
            assert!(record.cancelled_at.is_some() && record.claim.is_none());
            assert_eq!(
                record.outcome.expect("abandoned").cause,
                Some(crate::persistence::OutcomeCause::Abandoned)
            );
            assert_eq!(requests.load(Ordering::SeqCst), 0);
            wait(runtime.shutdown()).expect("shutdown");
            worker.shutdown().expect("shutdown");
        }

        #[test]
        fn t1_requires_execution_and_rejects_parent_dependency_cycles() {
            let (_root, directory) = data_directory("dispatcher-graph");
            let worker = DatabaseWorker::start(&directory).expect("database");
            let handle = worker.handle();
            let (target, _) = scripted(&handle, "dispatcher-graph", Vec::new());
            let mut missing = plan(&handle, &target, "missing execution", None).job;
            missing.configuration = None;
            assert!(matches!(
                wait(handle.create_job(missing)),
                Err(DatabaseError::Rejected(Violation::InvalidValue(
                    "execution",
                    _
                )))
            ));
            let root =
                wait(handle.create_job(plan(&handle, &target, "root", None).job)).expect("root");
            let mut child = plan(&handle, &target, "child", None).job;
            child.parent = root;
            let child = wait(handle.create_job(child)).expect("child");
            let mut depends_on_child = plan(&handle, &target, "root waiter", None).job;
            depends_on_child.dependencies = vec![(child, DependencyCondition::Settled)];
            wait(handle.create_job(depends_on_child)).expect("a root can wait on an older child");
            let mut child_waiter = plan(&handle, &target, "child waiter", None).job;
            child_waiter.parent = root;
            child_waiter.dependencies = vec![(root, DependencyCondition::Settled)];
            assert!(matches!(
                wait(handle.create_job(child_waiter)),
                Err(DatabaseError::Rejected(Violation::InvalidValue(
                    "dependencies",
                    _
                )))
            ));
            assert_eq!(wait(handle.jobs(None, 100)).expect("jobs").len(), 3);
            worker.shutdown().expect("shutdown");
        }

        #[test]
        fn admission_acknowledges_the_durably_started_job_before_the_provider_answers() {
            let (_root, directory) = data_directory("admission-early");
            let worker = DatabaseWorker::start(&directory).expect("start database");
            let handle = worker.handle();
            let (open, response) = gated(answering("the answer", (12, 3)));
            let (target, requests) = scripted(&handle, "admission-early", vec![response]);
            let runtime = ExecutionRuntime::start(handle.clone()).expect("start runtime");

            let job = admitted(wait(
                runtime
                    .scheduler()
                    .admit_provider(plan(&handle, &target, "early", None)),
            ));
            // Acknowledged: one durable Job, started and claimed, and not
            // settled, while its Provider has not answered.
            let [record] = jobs_named(&handle, "early").try_into().expect("one Job");
            assert_eq!(record.id, job.job());
            assert_eq!(record.input, job.input());
            assert_eq!(job.executor(), target.executor);
            assert!(
                record.assigned_executor.is_none()
                    || record.assigned_executor == Some(target.executor)
            );
            assert!(!settled(&record));
            let input = wait(handle.job_input(job.input()))
                .expect("read")
                .expect("the frozen input");
            assert_eq!(input.objective.as_deref(), Some("the objective of early"));
            assert!(
                summary(&handle, job.job()).is_none(),
                "nothing answered yet"
            );
            assert!(requests.load(Ordering::SeqCst) <= 1);

            // Settlement is awaited apart, and is of the same Job.
            open.send(()).expect("let the Provider answer");
            let (id, input_id) = (job.job(), job.input());
            let receipt = wait(job.settled()).expect("settled");
            assert_eq!((receipt.job, receipt.input), (id, input_id));
            assert!(job_record(&handle, id).completed_at.is_some());
            assert_eq!(summary(&handle, id).as_deref(), Some("the answer"));
            assert_eq!(requests.load(Ordering::SeqCst), 1);
            assert_eq!(
                jobs_named(&handle, "early").len(),
                1,
                "one Job, created once"
            );
            wait(runtime.shutdown()).expect("shutdown runtime");
            worker.shutdown().expect("shutdown");
            assert_relationships_consistent(&directory);
        }

        #[test]
        fn one_submission_is_one_job_with_its_input_outputs_and_tool_evidence() {
            let (_root, directory) = data_directory("admission-once");
            let worker = DatabaseWorker::start(&directory).expect("start database");
            let handle = worker.handle();
            let (target, _) = scripted(
                &handle,
                "admission-once",
                vec![
                    now(calling(&[add(0, "call_add", 2, 3)], (10, 4))),
                    now(answering("2 + 3 = 5", (20, 5))),
                    now(answering("legacy answer", (5, 2))),
                ],
            );
            let (registry, calls) = registered(vec![("add", Behavior::Add)]);
            let runtime = ExecutionRuntime::start(handle.clone()).expect("start runtime");
            let scheduler = runtime.scheduler();
            let before = wait(handle.jobs(None, 1_000)).expect("read").len();

            let tools = execution(&registry, &["add"], ToolLoopLimits::default());
            let job = admitted(wait(scheduler.admit_provider(plan(
                &handle,
                &target,
                "with tools",
                Some(tools),
            ))));
            let id = job.job();
            let receipt = wait(job.settled()).expect("settled");
            assert_eq!(receipt.job, id);
            assert_eq!(calls.load(Ordering::SeqCst), 1);
            let [record] = evidence(&handle, id).try_into().expect("one call");
            assert_eq!(record.call, tool_call(id, 1, 0));
            assert_eq!(outcome_of(&record), Some(completed(r#"{"sum":5}"#)));
            assert_eq!(summary(&handle, id).as_deref(), Some("2 + 3 = 5"));
            let [job_record] = jobs_named(&handle, "with tools")
                .try_into()
                .expect("one Job");
            assert_eq!(job_record.input, receipt.input);

            // The final-receipt API is the same path: one more Job, not two.
            let receipt = wait(scheduler.schedule_provider(plan(&handle, &target, "legacy", None)))
                .expect("settled");
            let [legacy] = jobs_named(&handle, "legacy").try_into().expect("one Job");
            assert_eq!((legacy.id, legacy.input), (receipt.job, receipt.input));
            assert!(legacy.completed_at.is_some());
            assert_eq!(
                summary(&handle, legacy.id).as_deref(),
                Some("legacy answer")
            );
            assert_eq!(
                wait(handle.jobs(None, 1_000)).expect("read").len(),
                before + 2
            );
            wait(runtime.shutdown()).expect("shutdown runtime");
            worker.shutdown().expect("shutdown");
            assert_relationships_consistent(&directory);
        }

        #[test]
        fn plans_that_never_start_are_refused_and_persist_no_job() {
            let (_root, directory) = data_directory("admission-refused");
            let worker = DatabaseWorker::start_with_config(&directory, lenient()).expect("start");
            let handle = worker.handle();
            let (target, requests) = scripted(&handle, "admission-refused", Vec::new());
            let runtime = ExecutionRuntime::start(handle.clone()).expect("start runtime");
            let scheduler = runtime.scheduler();
            let refused = |result: Result<AdmittedJob, AdmissionError>| match result {
                Err(AdmissionError::NotAdmitted(error)) => error,
                other => panic!("the plan must be refused, got {other:?}"),
            };

            let mut unknown_executor = plan(&handle, &target, "no executor", None);
            unknown_executor.executor_id = ExecutorId::new(999);
            assert!(matches!(
                refused(wait(scheduler.admit_provider(unknown_executor))),
                ExecutionRuntimeError::ExecutorNotFound(_)
            ));

            let (registry, _) = registered(vec![("add", Behavior::Add)]);
            let invalid = ToolLoopLimits {
                max_provider_requests: 0,
                ..ToolLoopLimits::default()
            };
            assert!(matches!(
                refused(wait(scheduler.admit_provider(plan(
                    &handle,
                    &target,
                    "no requests",
                    Some(execution(&registry, &["add"], invalid)),
                )))),
                ExecutionRuntimeError::InvalidToolLimits(_)
            ));
            let tools = execution(&registry, &["add"], ToolLoopLimits::default());
            registry.revoke("add").expect("revoke");
            assert!(matches!(
                refused(wait(scheduler.admit_provider(plan(
                    &handle,
                    &target,
                    "revoked",
                    Some(tools),
                )))),
                ExecutionRuntimeError::ToolsUnavailable(_)
            ));

            // Storage refuses the creation itself, once the input was saved.
            let (seam_handle, path) = (handle.clone(), directory.database_path().to_path_buf());
            provider_seam::before_creation_of("refused by storage", move || {
                protect(&seam_handle, &path);
            });
            let error = refused(wait(scheduler.admit_provider(plan(
                &handle,
                &target,
                "refused by storage",
                None,
            ))));
            let ExecutionRuntimeError::Database(error) = error else {
                panic!("a storage refusal, got {error:?}");
            };
            assert!(matches!(
                error.command_outcome(),
                Some(CommandOutcome::NotAccepted | CommandOutcome::NotCommitted)
            ));

            for name in [
                "no executor",
                "no requests",
                "revoked",
                "refused by storage",
            ] {
                assert!(
                    jobs_named(&handle, name).is_empty(),
                    "{name} created no Job"
                );
            }
            assert_eq!(requests.load(Ordering::SeqCst), 0, "nothing was sent");

            // Once shutdown has begun, nothing is admitted.
            wait(runtime.shutdown()).expect("shutdown runtime");
            assert!(matches!(
                refused(wait(scheduler.admit_provider(plan(
                    &handle,
                    &target,
                    "after shutdown",
                    None,
                )))),
                ExecutionRuntimeError::ShuttingDown
            ));
            worker.shutdown().expect("shutdown");
        }

        #[test]
        fn an_unknown_creation_commit_is_uncertain_and_never_resubmitted() {
            let (_root, directory) = data_directory("admission-unknown");
            let worker = DatabaseWorker::start_with_config(&directory, lenient()).expect("start");
            let handle = worker.handle();
            let (target, requests) = scripted(&handle, "admission-unknown", Vec::new());
            let companion_job = create_job(
                &handle,
                NewJob {
                    eligible_at: Some(Timestamp::from_unix_micros(
                        Timestamp::now().as_unix_micros() + 3_600_000_000,
                    )),
                    configuration: llm_only(target.llm),
                    ..new_job(&handle, "companion", handle.genesis(), Vec::new())
                },
            );
            let runtime = ExecutionRuntime::start(handle.clone()).expect("start runtime");
            let failed_group = Arc::new(std::sync::Mutex::new(None));
            let (group, seam_handle) = (failed_group.clone(), handle.clone());
            provider_seam::before_creation_of("unknown commit", move || {
                *group.lock().expect("lock") = Some(fail_next_commit(&seam_handle, companion_job));
            });

            let operation = match wait(runtime.scheduler().admit_provider(plan(
                &handle,
                &target,
                "unknown commit",
                None,
            ))) {
                Err(AdmissionError::Uncertain(ExecutionRuntimeError::Database(
                    DatabaseError::OutcomeUnknown { operation, .. },
                ))) => operation,
                other => panic!("an unknown commit is uncertain, not refused, got {other:?}"),
            };
            let companion = failed_group
                .lock()
                .expect("lock")
                .take()
                .expect("the commit was failed");
            assert!(matches!(
                companion.join().expect("joined"),
                Err(DatabaseError::OutcomeUnknown { .. })
            ));
            assert_eq!(requests.load(Ordering::SeqCst), 0, "nothing was sent");
            wait(runtime.shutdown()).expect("shutdown runtime");

            // Whatever the commit did, there is at most that one Job, and a
            // later start settles it if it exists; nothing created another.
            wait(handle.recover_storage()).expect("recover storage");
            let status = wait(handle.reconcile_operation(operation)).expect("reconcile");
            let jobs = jobs_named(&handle, "unknown commit");
            match status {
                crate::database::OperationStatus::Committed => {
                    let [record] = jobs.try_into().expect("the one Job");
                    // The recovery reopened the file, which settled the Job
                    // the earlier opening started as abandoned.
                    assert!(record.started_at.is_none() && record.cancelled_at.is_some());
                    assert_eq!(
                        record.outcome.expect("abandoned pending Job").cause,
                        Some(crate::persistence::OutcomeCause::Abandoned)
                    );
                    worker.shutdown().expect("shutdown");
                    let worker = DatabaseWorker::start(&directory).expect("restart");
                    assert!(worker.handle().abandoned_at_open().is_empty());
                    worker.shutdown().expect("shutdown");
                }
                _ => {
                    assert!(jobs.is_empty(), "no Job: {jobs:?}");
                    worker.shutdown().expect("shutdown");
                }
            }
        }

        #[test]
        fn a_stop_request_reaches_only_the_acknowledged_job() {
            let (_root, directory) = data_directory("admission-stop");
            let worker = DatabaseWorker::start(&directory).expect("start database");
            let handle = worker.handle();
            let (target, requests) = scripted(
                &handle,
                "admission-stop",
                vec![stalled(), now(answering("second answer", (3, 1)))],
            );
            let runtime = ExecutionRuntime::start(handle.clone()).expect("start runtime");
            let scheduler = runtime.scheduler();

            let first = admitted(wait(
                scheduler.admit_provider(plan(&handle, &target, "stopped", None)),
            ));
            until(|| requests.load(Ordering::SeqCst) == 1);
            // The next plan is admitted only once the executor is free.
            let second_plan = plan(&handle, &target, "untouched", None);
            let second_token = second_plan.provider_config.cancellation.clone();
            let next = scheduler.clone();
            let second = std::thread::spawn(move || wait(next.admit_provider(second_plan)));

            let stop = first.stop_handle();
            assert_eq!(stop.job(), first.job());
            stop.request();
            let first_id = first.job();
            let receipt = wait(first.settled()).expect("settled");
            assert_eq!((receipt.job, receipt.result), (first_id, None));
            let record = job_record(&handle, first_id);
            assert!(
                record.unknown_at.is_some()
                    && record.stop_confirmed_at.is_some()
                    && record.cancelled_at.is_none(),
                "a stopped request that reached the Provider is Unknown, not Cancelled"
            );

            let second = admitted(second.join().expect("joined"));
            assert_ne!(second.job(), first_id);
            // A repeated stop of the first Job reaches no other.
            stop.request();
            let second_id = second.job();
            wait(second.settled()).expect("settled");
            assert!(!second_token.is_cancelled());
            assert!(job_record(&handle, second_id).completed_at.is_some());
            assert_eq!(
                summary(&handle, second_id).as_deref(),
                Some("second answer")
            );
            wait(runtime.shutdown()).expect("shutdown runtime");
            worker.shutdown().expect("shutdown");
            assert_relationships_consistent(&directory);
        }

        #[test]
        fn a_stop_reaches_one_job_of_plans_sharing_a_token() {
            let (_root, directory) = data_directory("admission-shared-token");
            let worker = DatabaseWorker::start(&directory).expect("start database");
            let handle = worker.handle();
            let (open, response) = gated(answering("b answered", (3, 1)));
            let (target, requests) =
                scripted(&handle, "admission-shared-token", vec![stalled(), response]);
            let runtime = ExecutionRuntime::start(handle.clone()).expect("start runtime");
            let scheduler = runtime.scheduler();
            // Two plans of one cloned configuration share one token.
            let upstream = CancellationToken::default();
            let mut first = plan(&handle, &target, "job a", None);
            first.provider_config.cancellation = upstream.clone();
            let mut second = plan(&handle, &target, "job b", None);
            second.provider_config = first.provider_config.clone();

            let a = admitted(wait(scheduler.admit_provider(first)));
            until(|| requests.load(Ordering::SeqCst) == 1);
            let next = scheduler.clone();
            let b = std::thread::spawn(move || wait(next.admit_provider(second)));
            let stale = a.stop_handle();
            stale.request();
            let a_id = a.job();
            let receipt = wait(a.settled()).expect("settled");
            assert_eq!((receipt.job, receipt.result), (a_id, None));
            let record = job_record(&handle, a_id);
            assert!(record.unknown_at.is_some() && record.stop_confirmed_at.is_some());
            assert!(record.cancelled_at.is_none());
            assert!(
                !upstream.is_cancelled(),
                "a Job's stop is not its plan's token"
            );

            let b = admitted(b.join().expect("joined"));
            until(|| requests.load(Ordering::SeqCst) == 2);
            // The stale handle of Job A, while Job B waits for its answer.
            stale.request();
            open.send(()).expect("let the Provider answer");
            let b_id = b.job();
            wait(b.settled()).expect("settled");
            stale.request();
            let record = job_record(&handle, b_id);
            assert!(record.completed_at.is_some() && record.unknown_at.is_none());
            assert_eq!(summary(&handle, b_id).as_deref(), Some("b answered"));
            assert!(!upstream.is_cancelled());

            // The settled Jobs' cancellations are not kept by the token.
            drop(stale);
            let probe = upstream.child();
            assert_eq!(upstream.registered_children(), 1);
            drop(probe);
            wait(runtime.shutdown()).expect("shutdown runtime");
            worker.shutdown().expect("shutdown");
        }

        #[test]
        fn cancelling_a_shared_token_stops_each_job_by_its_own_rules() {
            let (_root, directory) = data_directory("admission-upstream");
            let worker = DatabaseWorker::start(&directory).expect("start database");
            let handle = worker.handle();
            let (target, requests) = scripted(&handle, "admission-upstream", vec![stalled()]);
            let runtime = ExecutionRuntime::start(handle.clone()).expect("start runtime");
            let scheduler = runtime.scheduler();
            let upstream = CancellationToken::default();
            let sharing = |name: &str| {
                let mut plan = plan(&handle, &target, name, None);
                plan.provider_config.cancellation = upstream.clone();
                plan
            };

            let running = admitted(wait(scheduler.admit_provider(sharing("delivered"))));
            until(|| requests.load(Ordering::SeqCst) == 1);
            let next = scheduler.clone();
            let queued_plan = sharing("queued");
            let queued = std::thread::spawn(move || wait(next.admit_provider(queued_plan)));
            upstream.cancel();

            // The running request may have reached the Provider: Unknown.
            let id = running.job();
            wait(running.settled()).expect("settled");
            let record = job_record(&handle, id);
            assert!(record.unknown_at.is_some() && record.stop_confirmed_at.is_some());
            assert!(record.cancelled_at.is_none());

            // A Job started after the cancellation sends nothing: Failed.
            let later = admitted(wait(scheduler.admit_provider(sharing("later"))));
            for job in [admitted(queued.join().expect("joined")), later] {
                let id = job.job();
                wait(job.settled()).expect("settled");
                let record = job_record(&handle, id);
                assert!(
                    (record.failed_at.is_some() || record.cancelled_at.is_some())
                        && record.unknown_at.is_none(),
                    "{record:?}"
                );
            }
            assert_eq!(requests.load(Ordering::SeqCst), 1, "nothing else was sent");
            wait(runtime.shutdown()).expect("shutdown runtime");
            worker.shutdown().expect("shutdown");
        }

        #[test]
        fn a_stop_after_settlement_changes_nothing() {
            let (_root, directory) = data_directory("admission-late-stop");
            let worker = DatabaseWorker::start(&directory).expect("start database");
            let handle = worker.handle();
            let (target, _) = scripted(
                &handle,
                "admission-late-stop",
                vec![now(answering("done", (3, 1)))],
            );
            let runtime = ExecutionRuntime::start(handle.clone()).expect("start runtime");
            let job = admitted(wait(
                runtime
                    .scheduler()
                    .admit_provider(plan(&handle, &target, "finished", None)),
            ));
            let (id, stop) = (job.job(), job.stop_handle());
            wait(job.settled()).expect("settled");
            let before = job_record(&handle, id);
            stop.request();
            wait(runtime.shutdown()).expect("shutdown runtime");
            let after = job_record(&handle, id);
            assert!(after.completed_at.is_some());
            assert_eq!(before, after, "a completed Job is not cancelled afterwards");
            worker.shutdown().expect("shutdown");
        }

        #[test]
        fn a_stop_during_a_non_idempotent_call_leaves_its_effect_uncertain() {
            let (_root, directory) = data_directory("admission-tool-stop");
            let worker = DatabaseWorker::start(&directory).expect("start database");
            let handle = worker.handle();
            let (target, _) = scripted(
                &handle,
                "admission-tool-stop",
                vec![now(calling(
                    &[call(0, "call_hang", "hang", r#"{"a":1,"b":2}"#)],
                    (5, 2),
                ))],
            );
            let (registry, calls) =
                registered_as(ToolEffect::NonIdempotent, vec![("hang", Behavior::Hang)]);
            let runtime = ExecutionRuntime::start(handle.clone()).expect("start runtime");
            let tools = execution(&registry, &["hang"], ToolLoopLimits::default());
            let job = admitted(wait(runtime.scheduler().admit_provider(plan(
                &handle,
                &target,
                "effectful",
                Some(tools),
            ))));
            until(|| calls.load(Ordering::SeqCst) == 1);
            job.request_stop();
            let id = job.job();
            match wait(job.settled()) {
                Err(ExecutionRuntimeError::ToolLoopStopped { receipt, stop, .. }) => {
                    assert_eq!(receipt.job, id);
                    assert_eq!(stop, ToolLoopStop::Cancelled);
                }
                other => panic!("the tool loop stops on the cancellation, got {other:?}"),
            }
            let [record] = evidence(&handle, id).try_into().expect("one call");
            assert_eq!(
                outcome_of(&record),
                Some(ToolOutcome::Uncertain {
                    cause: UncertainCause::Cancelled,
                    message: None,
                })
            );
            let job_record = job_record(&handle, id);
            assert!(job_record.unknown_at.is_some() && job_record.cancelled_at.is_none());
            assert_eq!(
                violation(wait(handle.retry_job(retry_of(id, target.llm, true)))),
                Violation::UnresolvedToolEffect(record.call),
                "the effect stays unresolved"
            );
            wait(runtime.shutdown()).expect("shutdown runtime");
            worker.shutdown().expect("shutdown");
        }

        #[test]
        fn shutdown_and_a_stop_request_settle_the_job_once_and_refuse_queued_plans() {
            let (_root, directory) = data_directory("admission-shutdown");
            let worker = DatabaseWorker::start(&directory).expect("start database");
            let handle = worker.handle();
            let (target, requests) = scripted(&handle, "admission-shutdown", vec![stalled()]);
            let runtime = ExecutionRuntime::start(handle.clone()).expect("start runtime");
            let scheduler = runtime.scheduler();
            let job = admitted(wait(
                scheduler.admit_provider(plan(&handle, &target, "running", None)),
            ));
            until(|| requests.load(Ordering::SeqCst) == 1);
            let queued = admit(scheduler.admit_provider(plan(&handle, &target, "queued", None)));

            let stop = job.stop_handle();
            let stopping = std::thread::spawn(move || stop.request());
            wait(runtime.shutdown()).expect("shutdown runtime");
            stopping.join().expect("joined");
            match queued() {
                Err(AdmissionError::NotAdmitted(ExecutionRuntimeError::ShuttingDown)) => {
                    assert!(jobs_named(&handle, "queued").is_empty())
                }
                Ok(queued) => {
                    let id = queued.job();
                    wait(queued.settled()).expect("settled on shutdown");
                    assert!(job_record(&handle, id).any_terminal());
                }
                other => panic!("shutdown must settle or refuse: {other:?}"),
            }
            let id = job.job();
            let receipt = wait(job.settled()).expect("settled once, by the executor");
            assert_eq!((receipt.job, receipt.result), (id, None));
            let record = job_record(&handle, id);
            assert!(record.unknown_at.is_some() && record.stop_confirmed_at.is_some());
            assert!(record.completed_at.is_none() && record.failed_at.is_none());
            assert!(record.cancelled_at.is_none());
            worker.shutdown().expect("shutdown");
            let worker = DatabaseWorker::start(&directory).expect("restart");
            assert!(worker.handle().abandoned_at_open().is_empty());
            worker.shutdown().expect("shutdown");
        }

        #[test]
        fn dropping_the_admitted_job_neither_stops_nor_orphans_its_execution() {
            let (_root, directory) = data_directory("admission-dropped");
            let worker = DatabaseWorker::start(&directory).expect("start database");
            let handle = worker.handle();
            let (open, response) = gated(answering("finished alone", (4, 2)));
            let (target, _) = scripted(&handle, "admission-dropped", vec![response]);
            let runtime = ExecutionRuntime::start(handle.clone()).expect("start runtime");
            let plan = plan(&handle, &target, "unwatched", None);
            let token = plan.provider_config.cancellation.clone();
            let job = admitted(wait(runtime.scheduler().admit_provider(plan)));
            let id = job.job();
            // Its settlement is waited for, then abandoned, then the handle dropped.
            let mut waiting = Box::pin(job.settled());
            assert!(poll_once(waiting.as_mut()).is_none());
            drop(waiting);
            assert!(!token.is_cancelled());
            assert!(!settled(&job_record(&handle, id)));

            open.send(()).expect("let the Provider answer");
            until(|| job_record(&handle, id).completed_at.is_some());
            assert_eq!(summary(&handle, id).as_deref(), Some("finished alone"));
            wait(runtime.shutdown()).expect("shutdown runtime");
            worker.shutdown().expect("shutdown");
        }

        #[test]
        fn a_hosted_caller_is_acknowledged_early_and_awaits_settlement_apart() {
            let (_root, directory) = data_directory("admission-hosted");
            let (open, response) = gated(answering("hosted answer", (6, 2)));
            let endpoint = "admission-hosted";
            let requests = Arc::new(AtomicUsize::new(0));
            let counted = requests.clone();
            let mut responses = vec![response, stalled()].into_iter();
            let (delivered, reached) = flume::unbounded();
            provider_seam::install(&format!("https://{endpoint}.invalid/v1"), move |_| {
                counted.fetch_add(1, Ordering::SeqCst);
                delivered.send(()).ok();
                responses.next().expect("a scripted response")
            });
            let jobs = Arc::new(std::sync::Mutex::new(Vec::new()));
            let seen = jobs.clone();
            let report =
                crate::host::run_hosted_reporting(&directory, false, move |context| async move {
                    let database = context.database();
                    let provider: LlmProviderId = database
                        .create_config(NewConfig {
                            name: endpoint.into(),
                            body: ConfigBody::Connection(ConnectionConfig {
                                protocol: OPENAI,
                                endpoint: Some(format!("https://{endpoint}.invalid/v1")),
                                credential_ref: None,
                            }),
                        })
                        .await
                        .expect("create Connection")
                        .into();
                    let hosted = llm_config("hosted", &[ModelCapability::Chat]);
                    let llm: ProfileId = database
                        .create_config(NewConfig {
                            name: "hosted".into(),
                            body: ConfigBody::Model(ModelConfig {
                                connection_id: provider.into(),
                                model: hosted.model,
                                context_window: hosted.context_window,
                                parameters: hosted.parameters,
                                declared_capabilities: hosted.declared_capabilities,
                            }),
                        })
                        .await
                        .expect("create Model config")
                        .into();
                    let executor = database
                        .create_executor(NewExecutor {
                            name: "hosted worker".into(),
                            concurrency_limit: NonZeroU32::MIN,
                            llm_transports: BTreeSet::from([OPENAI]),
                        })
                        .await
                        .expect("create executor");
                    let target = Target { executor, llm };
                    let job = context
                        .scheduler()
                        .admit_provider(plan(database, &target, "hosted", None))
                        .await
                        .expect("admitted");
                    let record = database
                        .job(job.job())
                        .await
                        .expect("read")
                        .expect("the Job");
                    assert!(!settled(&record));
                    assert_eq!(record.input, job.input());
                    open.send(()).expect("let the Provider answer");
                    let id = job.job();
                    let receipt = job.settled().await.expect("settled");
                    assert_eq!(receipt.job, id);
                    // A second Job is admitted and left running when the host
                    // stops; the host's shutdown, not its caller, settles it.
                    let left = context
                        .scheduler()
                        .admit_provider(plan(database, &target, "left running", None))
                        .await
                        .expect("admitted");
                    seen.lock().expect("lock").extend([id, left.job()]);
                    drop(left);
                    // Its request reached the Provider before the host stops.
                    for _ in 0..2 {
                        reached.recv_async().await.expect("a request");
                    }
                });
            // The process-wide logger may already be another host's in this
            // test binary; that is reported, and nothing else may fail.
            assert!(
                report
                    .failures
                    .iter()
                    .all(|failure| failure.phase == crate::host::Phase::Logger),
                "{:?}",
                report.failures
            );
            assert_eq!(requests.load(Ordering::SeqCst), 2);

            let worker = DatabaseWorker::start(&directory).expect("reopen");
            let handle = worker.handle();
            assert!(
                handle.abandoned_at_open().is_empty(),
                "nothing was left unsettled"
            );
            let [answered, left] = jobs.lock().expect("lock").clone().try_into().expect("two");
            assert!(job_record(&handle, answered).completed_at.is_some());
            assert_eq!(summary(&handle, answered).as_deref(), Some("hosted answer"));
            let record = job_record(&handle, left);
            assert!(record.unknown_at.is_some() && record.stop_confirmed_at.is_some());
            worker.shutdown().expect("shutdown");
        }
    }

    // ---- Control Plane submissions ----------------------------------------------

    mod control_plane_submissions {
        use std::collections::BTreeMap;
        use std::io::{Read, Write};
        use std::net::{SocketAddr, TcpStream};

        use super::*;
        use crate::control_plane::{
            ControlPlane, ControlPlaneConfig, MAX_SUBMISSION_BYTES, resolution_seam,
        };
        use crate::host::{HostContext, Phase, run_controlled_reporting};
        use crate::orchestration::execution_runtime::provider_seam::{self, Response};
        use crate::provider_execution::scripted::Step;

        const TOKEN: &str = "C2-Submission-Token_0123456789.abcdefghijk~";

        fn config(admission_wait: Duration) -> ControlPlaneConfig {
            ControlPlaneConfig::new(0, TOKEN)
                .expect("a valid configuration")
                .with_admission_wait(admission_wait)
        }

        /// One HTTP/1.1 response.
        struct Answer {
            status: u16,
            headers: BTreeMap<String, String>,
            body: String,
        }

        impl Answer {
            fn json(&self) -> Value {
                serde_json::from_str(&self.body)
                    .unwrap_or_else(|_| panic!("a JSON body: {}", self.body))
            }

            fn error(&self) -> String {
                self.json()["error"]["code"]
                    .as_str()
                    .unwrap_or_else(|| panic!("an error code: {}", self.body))
                    .to_owned()
            }

            fn admission(&self) -> (String, Option<String>) {
                let admission = &self.json()["admission"];
                (
                    admission["status"]
                        .as_str()
                        .unwrap_or_else(|| panic!("an admission: {}", self.body))
                        .to_owned(),
                    admission["job"].as_str().map(str::to_owned),
                )
            }

            fn job(&self) -> JobId {
                let (status, job) = self.admission();
                assert_eq!(status, "admitted", "{}", self.body);
                job.and_then(|job| job.strip_prefix("job:")?.parse().ok())
                    .map(JobId::new)
                    .expect("an admitted Job")
            }

            fn stop(&self) -> String {
                self.json()["stop"]["status"]
                    .as_str()
                    .unwrap_or_else(|| panic!("a stop: {}", self.body))
                    .to_owned()
            }
        }

        fn parse(raw: &[u8]) -> Answer {
            let raw = String::from_utf8(raw.to_vec()).expect("UTF-8");
            let (head, body) = raw.split_once("\r\n\r\n").expect("a head");
            let mut lines = head.lines();
            let status = lines
                .next()
                .and_then(|line| line.split(' ').nth(1))
                .and_then(|code| code.parse().ok())
                .expect("a status");
            let headers = lines
                .filter_map(|line| line.split_once(": "))
                .map(|(name, value)| (name.to_ascii_lowercase(), value.to_owned()))
                .collect();
            Answer {
                status,
                headers,
                body: body.to_owned(),
            }
        }

        /// Writes `request_line` with exactly `headers` and `body` on a new
        /// connection, and returns the connection.
        fn open(
            address: SocketAddr,
            request_line: &str,
            headers: &[(&str, &str)],
            body: &[u8],
        ) -> TcpStream {
            let mut stream = TcpStream::connect(address).expect("connect");
            stream
                .set_read_timeout(Some(crate::test_support::WAIT))
                .expect("timeout");
            let mut request = format!("{request_line} HTTP/1.1\r\n");
            for (name, value) in headers {
                request.push_str(&format!("{name}: {value}\r\n"));
            }
            request.push_str("Connection: close\r\n\r\n");
            let mut bytes = request.into_bytes();
            bytes.extend_from_slice(body);
            stream.write_all(&bytes).expect("send");
            stream
        }

        fn send(
            address: SocketAddr,
            request_line: &str,
            headers: &[(&str, &str)],
            body: &[u8],
        ) -> Answer {
            let mut stream = open(address, request_line, headers, body);
            let mut raw = Vec::new();
            stream.read_to_end(&mut raw).expect("read");
            parse(&raw)
        }

        fn host(address: SocketAddr) -> String {
            format!("127.0.0.1:{}", address.port())
        }

        fn bearer() -> String {
            format!("Bearer {TOKEN}")
        }

        /// The request a well-behaved client sends to submit `body`.
        fn submission(address: SocketAddr, key: &str, body: &str) -> TcpStream {
            let (host, bearer, length) = (host(address), bearer(), body.len().to_string());
            open(
                address,
                "POST /api/v1/jobs",
                &[
                    ("Host", &host),
                    ("Authorization", &bearer),
                    ("Idempotency-Key", key),
                    ("Content-Type", "application/json"),
                    ("Content-Length", &length),
                ],
                body.as_bytes(),
            )
        }

        fn submit(address: SocketAddr, key: &str, body: &str) -> Answer {
            let mut stream = submission(address, key, body);
            let mut raw = Vec::new();
            stream.read_to_end(&mut raw).expect("read");
            parse(&raw)
        }

        fn lookup(address: SocketAddr, key: &str) -> Answer {
            let (host, bearer) = (host(address), bearer());
            send(
                address,
                "GET /api/v1/admissions",
                &[
                    ("Host", &host),
                    ("Authorization", &bearer),
                    ("Idempotency-Key", key),
                ],
                b"",
            )
        }

        fn stop(address: SocketAddr, job: JobId) -> Answer {
            let (host, bearer) = (host(address), bearer());
            send(
                address,
                &format!("POST /api/v1/jobs/{job}/stop"),
                &[("Host", &host), ("Authorization", &bearer)],
                b"",
            )
        }

        fn get(address: SocketAddr, path: &str) -> Answer {
            let (host, bearer) = (host(address), bearer());
            send(
                address,
                &format!("GET {path}"),
                &[("Host", &host), ("Authorization", &bearer)],
                b"",
            )
        }

        fn state(address: SocketAddr, job: JobId) -> String {
            let answer = get(address, &format!("/api/v1/jobs/{job}"));
            assert_eq!(answer.status, 200, "{}", answer.body);
            answer.json()["job"]["settlement"]["state"]
                .as_str()
                .expect("a state")
                .to_owned()
        }

        fn until(mut condition: impl FnMut() -> bool) {
            let deadline = std::time::Instant::now() + crate::test_support::WAIT;
            while !condition() {
                assert!(std::time::Instant::now() < deadline, "not reached in time");
                std::thread::sleep(Duration::from_millis(5));
            }
        }

        fn now(steps: Vec<Step>) -> Response {
            Response { gate: None, steps }
        }

        fn gated(steps: Vec<Step>) -> (flume::Sender<()>, Response) {
            let (open, gate) = flume::bounded(1);
            (
                open,
                Response {
                    gate: Some(gate),
                    steps,
                },
            )
        }

        fn stalled() -> Response {
            now(vec![Step::Stall])
        }

        /// An LLM Profile on a Provider answered by the test, an Executor
        /// that runs it, and the count of requests the Provider received.
        struct Target {
            llm: ProfileId,
            executor: ExecutorId,
            requests: Arc<AtomicUsize>,
        }

        impl Target {
            fn body(&self, objective: &str, name: &str) -> String {
                json!({
                    "llm_profile": self.llm.to_string(),
                    "executor": self.executor.to_string(),
                    "objective": objective,
                    "name": name,
                })
                .to_string()
            }

            fn requests(&self) -> usize {
                self.requests.load(Ordering::SeqCst)
            }
        }

        fn target(handle: &DatabaseHandle, name: &str, responses: Vec<Response>) -> Target {
            let provider = provider(handle, name);
            let llm = llm_profile(handle, provider, llm_config(name, &[ModelCapability::Chat]));
            let executor = executor(handle, "llm worker", &[OPENAI]).expect("register");
            let requests = Arc::new(AtomicUsize::new(0));
            let counted = requests.clone();
            let mut responses = responses.into_iter();
            provider_seam::install(&format!("https://{name}.invalid/v1"), move |_| {
                counted.fetch_add(1, Ordering::SeqCst);
                responses
                    .next()
                    .expect("a scripted response for every request")
            });
            Target {
                llm,
                executor,
                requests,
            }
        }

        /// The real database, execution runtime and listener, started as
        /// the host starts them.
        struct Running {
            worker: DatabaseWorker,
            handle: DatabaseHandle,
            runtime: ExecutionRuntime,
            control: ControlPlane,
        }

        impl Running {
            fn start(directory: &DataDirectory, admission_wait: Duration) -> Self {
                let worker =
                    DatabaseWorker::start_with_config(directory, lenient()).expect("start");
                let handle = worker.handle();
                let runtime = ExecutionRuntime::start(handle.clone()).expect("start runtime");
                let control = ControlPlane::start(
                    config(admission_wait),
                    handle.clone(),
                    runtime.scheduler(),
                )
                .expect("start listener");
                Self {
                    worker,
                    handle,
                    runtime,
                    control,
                }
            }

            fn address(&self) -> SocketAddr {
                self.control.address()
            }

            /// Stops in the host's order: listener, runtime, database.
            fn stop(self) {
                wait(self.control.stop()).expect("stop the listener");
                wait(self.runtime.shutdown()).expect("shutdown runtime");
                self.worker.shutdown().expect("shutdown");
            }
        }

        /// `headers` with each of `extra` in place of any of its name.
        fn replaced<'a>(
            headers: &[(&'a str, &'a str)],
            extra: &[(&'a str, &'a str)],
        ) -> Vec<(&'a str, &'a str)> {
            let mut headers = headers.to_vec();
            for (name, value) in extra {
                headers.retain(|(existing, _)| existing != name);
                headers.push((name, value));
            }
            headers
        }

        fn settled(record: &JobRecord) -> bool {
            record.completed_at.is_some()
                || record.failed_at.is_some()
                || record.cancelled_at.is_some()
                || record.unknown_at.is_some()
        }

        /// Submits `body` under `key` and returns its Job once admitted,
        /// looking the key up while the answer was pending.
        fn admitted(address: SocketAddr, key: &str, body: &str) -> JobId {
            let answer = submit(address, key, body);
            match answer.status {
                200 | 201 => answer.job(),
                202 => {
                    until(|| lookup(address, key).admission().0 == "admitted");
                    lookup(address, key).job()
                }
                _ => panic!("not admitted: {}", answer.body),
            }
        }

        fn jobs_named(handle: &DatabaseHandle, name: &str) -> Vec<JobRecord> {
            wait(handle.jobs(None, 1_000))
                .expect("read Jobs")
                .into_iter()
                .filter(|job| job.name.as_deref() == Some(name))
                .collect()
        }

        const LONG: Duration = Duration::from_secs(5);
        const SHORT: Duration = Duration::from_millis(200);

        /// One Provider with two LLM Profiles, an Agent Profile that targets
        /// only the first and an Executor, with every request the Provider
        /// received recorded as sent.
        struct Agented {
            targeted: ProfileId,
            untargeted: ProfileId,
            agent: ProfileId,
            executor: ExecutorId,
            conversations: Arc<std::sync::Mutex<Vec<Value>>>,
        }

        fn agented(handle: &DatabaseHandle, name: &str) -> Agented {
            let provider = provider(handle, name);
            let targeted =
                llm_profile(handle, provider, llm_config(name, &[ModelCapability::Chat]));
            let untargeted = llm_profile(
                handle,
                provider,
                llm_config(&format!("{name}-other"), &[ModelCapability::Chat]),
            );
            let agent = agent_profile(handle, "Answer in one sentence.", vec![targeted]);
            let executor = executor(handle, "llm worker", &[OPENAI]).expect("register");
            let conversations = Arc::new(std::sync::Mutex::new(Vec::new()));
            let recorded = conversations.clone();
            provider_seam::install(&format!("https://{name}.invalid/v1"), move |conversation| {
                recorded
                    .lock()
                    .expect("lock")
                    .push(serde_json::to_value(conversation).expect("a serializable request"));
                now(answering("Hello from the Agent", (5, 2)))
            });
            Agented {
                targeted,
                untargeted,
                agent,
                executor,
                conversations,
            }
        }

        #[test]
        fn a_submission_may_name_an_agent_profile_that_targets_its_llm_profile() {
            let (_root, directory) = data_directory("c2-agent-submission");
            let running = Running::start(&directory, LONG);
            let handle = running.handle.clone();
            let configured = agented(&handle, "c2-agent");
            let address = running.address();
            let body = |agent: &str, llm: ProfileId, name: &str| {
                json!({
                    "agent_profile": agent,
                    "llm_profile": llm.to_string(),
                    "executor": configured.executor.to_string(),
                    "objective": "Say hello",
                    "name": name,
                })
                .to_string()
            };

            let job = admitted(
                address,
                "c2-agent-0000000000001",
                &body(
                    &configured.agent.to_string(),
                    configured.targeted,
                    "c2 agent job",
                ),
            );
            until(|| state(address, job) == "completed");
            let submitted = wait(handle.job_configuration(job))
                .expect("read")
                .expect("a configured Job");
            assert_eq!(submitted.agent_profile, Some(configured.agent));
            assert_eq!(submitted.llm_profile, configured.targeted);
            assert_eq!(
                submitted
                    .frozen
                    .agent
                    .and_then(|agent| agent.instructions)
                    .as_deref(),
                Some("Answer in one sentence.")
            );
            let sent = configured.conversations.lock().expect("lock").clone();
            let [request] = sent.as_slice() else {
                panic!("one Provider request, got {}", sent.len());
            };
            let sent = request.to_string();
            assert!(sent.contains("Answer in one sentence."), "{sent}");
            assert!(sent.contains("Say hello"), "{sent}");

            // The same key and request replay the Job; the Agent Profile is part of the request.
            let replay = submit(
                address,
                "c2-agent-0000000000001",
                &body(
                    &configured.agent.to_string(),
                    configured.targeted,
                    "c2 agent job",
                ),
            );
            assert_eq!((replay.status, replay.job()), (200, job), "{}", replay.body);
            let conflict = submit(
                address,
                "c2-agent-0000000000001",
                &body("profile:999999", configured.targeted, "c2 agent job"),
            );
            assert_eq!(conflict.error(), "idempotency_key_conflict");

            let refusals = [
                (
                    "c2-agent-0000000000002",
                    "profile:999999".to_owned(),
                    configured.targeted,
                    "agent_profile_not_found",
                ),
                (
                    "c2-agent-0000000000003",
                    configured.targeted.to_string(),
                    configured.targeted,
                    "agent_profile_not_found",
                ),
                (
                    "c2-agent-0000000000004",
                    configured.agent.to_string(),
                    configured.untargeted,
                    "llm_profile_not_targeted",
                ),
            ];
            for (key, agent, llm, code) in refusals {
                let refused = submit(address, key, &body(&agent, llm, "c2 refused"));
                assert_eq!(refused.status, 422, "{}", refused.body);
                assert_eq!(refused.error(), code);
            }
            assert!(jobs_named(&handle, "c2 refused").is_empty());
            assert_eq!(configured.conversations.lock().expect("lock").len(), 1);
            running.stop();
        }

        #[test]
        fn a_jobs_input_is_read_as_text_and_names_no_provider_or_credential() {
            let (_root, directory) = data_directory("c2-job-input");
            let running = Running::start(&directory, LONG);
            let handle = running.handle.clone();
            let configured = agented(&handle, "c2-input");
            let address = running.address();
            let objective = "Line one\nLine two with é and \"quotes\"";
            let with_agent = admitted(
                address,
                "c2-input-00000000001",
                &json!({
                    "agent_profile": configured.agent.to_string(),
                    "llm_profile": configured.targeted.to_string(),
                    "executor": configured.executor.to_string(),
                    "objective": objective,
                })
                .to_string(),
            );
            let without_agent = admitted(
                address,
                "c2-input-00000000002",
                &json!({
                    "llm_profile": configured.targeted.to_string(),
                    "executor": configured.executor.to_string(),
                    "objective": "Plain",
                })
                .to_string(),
            );
            until(|| state(address, with_agent) == "completed");
            until(|| state(address, without_agent) == "completed");

            let answer = get(address, &format!("/api/v1/jobs/{with_agent}/input"));
            assert_eq!(answer.status, 200, "{}", answer.body);
            let input = answer.json()["input"].clone();
            assert_eq!(input["objective"], objective);
            assert_eq!(input["attachments"], json!([]));
            assert_eq!(
                input["configuration"],
                json!({
                    "agent_profile": configured.agent.to_string(),
                    "llm_profile": configured.targeted.to_string(),
                    "model": "c2-input",
                    "context_window": Value::Null,
                })
            );
            let record = wait(handle.job(with_agent)).expect("read").expect("a Job");
            assert_eq!(input["id"], record.input.to_string());
            for secret in ["invalid", "C2-INPUT_KEY", "endpoint", "credential"] {
                assert!(!answer.body.contains(secret), "{secret}: {}", answer.body);
            }
            let plain = get(address, &format!("/api/v1/jobs/{without_agent}/input")).json();
            assert_eq!(plain["input"]["objective"], "Plain");
            assert_eq!(
                plain["input"]["configuration"]["agent_profile"],
                Value::Null
            );

            // The root of the Job tree is not a Job and has no input.
            let genesis = get(
                address,
                &format!("/api/v1/jobs/{}/input", running.handle.genesis()),
            );
            assert_eq!(genesis.status, 404, "{}", genesis.body);
            assert_eq!(get(address, "/api/v1/jobs/job:999999/input").status, 404);
            assert_eq!(get(address, "/api/v1/jobs/job:1/input?x=1").status, 400);
            running.stop();
        }

        #[test]
        fn agent_profiles_are_listed_with_their_llm_targets_and_executors() {
            let (_root, directory) = data_directory("c2-agent-list");
            let running = Running::start(&directory, LONG);
            let handle = running.handle.clone();
            let configured = agented(&handle, "c2-list");
            let second = agent_profile(
                &handle,
                "Another.",
                vec![configured.targeted, configured.untargeted],
            );
            let address = running.address();

            let listed = get(address, "/api/v1/agent-profiles");
            assert_eq!(listed.status, 200, "{}", listed.body);
            let page = listed.json();
            assert_eq!(page["has_more"], false);
            let agents = page["agent_profiles"].as_array().expect("a list");
            // Newest first; the LLM Profiles are not Agent Profiles.
            let ids = agents
                .iter()
                .map(|agent| agent["id"].clone())
                .collect::<Vec<_>>();
            assert_eq!(
                ids,
                [
                    json!(second.to_string()),
                    json!(configured.agent.to_string())
                ]
            );
            let targets = agents[0]["llm_targets"].as_array().expect("targets");
            assert_eq!(targets.len(), 2);
            assert_eq!(targets[0]["id"], configured.targeted.to_string());
            assert_eq!(targets[0]["model"], "c2-list");
            assert_eq!(targets[0]["declared_capabilities"], json!(["chat"]));
            assert_eq!(
                targets[0]["executors"],
                json!([configured.executor.to_string()])
            );
            for secret in ["invalid", "C2-LIST_KEY", "endpoint", "credential"] {
                assert!(!listed.body.contains(secret), "{secret}: {}", listed.body);
            }

            // A page reads `limit` Profiles of either kind, newest first, and continues from its oldest.
            let first = get(address, "/api/v1/agent-profiles?limit=1").json();
            assert_eq!(first["agent_profiles"][0]["id"], second.to_string());
            assert_eq!(first["has_more"], true);
            let cursor = first["next_before"].as_str().expect("a cursor").to_owned();
            let mut seen = vec![second.to_string()];
            let mut before = cursor;
            for _ in 0..8 {
                let next = get(
                    address,
                    &format!("/api/v1/agent-profiles?limit=1&before={before}"),
                )
                .json();
                for agent in next["agent_profiles"].as_array().expect("a list") {
                    seen.push(agent["id"].as_str().expect("an id").to_owned());
                }
                match next["next_before"].as_str() {
                    Some(cursor) => before = cursor.to_owned(),
                    None => break,
                }
            }
            assert_eq!(seen, [second.to_string(), configured.agent.to_string()]);

            for refused in [
                "?limit=0",
                "?limit=101",
                "?before=7",
                "?after=profile:1",
                "?limit",
            ] {
                let answer = get(address, &format!("/api/v1/agent-profiles{refused}"));
                assert_eq!(
                    (answer.status, answer.error().as_str()),
                    (400, "invalid_request"),
                    "{refused}"
                );
            }
            let (host, address_host) = (host(address), address);
            let anonymous = send(
                address_host,
                "GET /api/v1/agent-profiles",
                &[("Host", &host)],
                b"",
            );
            assert_eq!(anonymous.status, 401, "{}", anonymous.body);
            running.stop();
        }

        #[test]
        fn a_submission_is_admitted_once_and_answered_before_its_provider() {
            let (_root, directory) = data_directory("c2-admission");
            let running = Running::start(&directory, LONG);
            let handle = running.handle.clone();
            let (open, response) = gated(answering("hello there", (5, 2)));
            let target = target(&handle, "c2-admission", vec![response]);
            let address = running.address();
            let body = target.body("Say hello", "c2 ordinary");
            let key = "c2-ordinary-0000000001";

            let created = submit(address, key, &body);
            assert_eq!(created.status, 201, "{}", created.body);
            let job = created.job();
            assert_eq!(
                created.headers.get("cache-control").map(String::as_str),
                Some("no-store")
            );
            // Acknowledged while the Provider has not answered: one durable
            // Job, started and claimed, with its frozen configuration.
            let [record] = jobs_named(&handle, "c2 ordinary")
                .try_into()
                .expect("one Job");
            assert_eq!(record.id, job);
            assert!(!settled(&record));
            assert!(
                record.assigned_executor.is_none()
                    || record.assigned_executor == Some(target.executor)
            );
            assert_eq!(record.parent, handle.genesis());
            let input = wait(handle.job_input(record.input))
                .expect("read")
                .expect("its input");
            assert_eq!(input.objective.as_deref(), Some("Say hello"));
            assert!(input.submitted_at.is_some());
            let configuration = wait(handle.job_configuration(job))
                .expect("read")
                .expect("frozen");
            assert_eq!(configuration.llm_profile, target.llm);
            assert_eq!(configuration.frozen.llm.config.model, "c2-admission");
            assert_eq!(
                configuration.frozen.llm.provider.endpoint.as_deref(),
                Some("https://c2-admission.invalid/v1")
            );
            assert_eq!(state(address, job), "unsettled");
            assert_eq!(
                lookup(address, key).admission(),
                ("admitted".to_owned(), Some(job.to_string()))
            );
            assert_eq!(running.control.held().0, 1, "its stop is held");

            open.send(()).expect("let the Provider answer");
            until(|| state(address, job) == "completed");
            let outputs = get(address, &format!("/api/v1/jobs/{job}/outputs")).json();
            let outputs = outputs["outputs"].as_array().expect("outputs").clone();
            assert_eq!(outputs.last().expect("a result")["kind"], "result");
            assert_eq!(outputs.last().expect("a result")["summary"], "hello there");
            assert_eq!(target.requests(), 1);
            until(|| running.control.held() == (0, 0));

            // A replay names the same Job and runs nothing.
            let replay = submit(address, key, &body);
            assert_eq!((replay.status, replay.job()), (200, job));
            assert_eq!(jobs_named(&handle, "c2 ordinary").len(), 1);
            assert_eq!(target.requests(), 1);
            assert!(!replay.body.contains(key) && !replay.body.contains(TOKEN));
            running.stop();
            assert_relationships_consistent(&directory);
        }

        #[test]
        fn concurrent_requests_with_one_key_create_one_job_and_one_execution() {
            let (_root, directory) = data_directory("c2-concurrent");
            let running = Running::start(&directory, LONG);
            let handle = running.handle.clone();
            let (open, response) = gated(answering("once", (3, 1)));
            let target = target(&handle, "c2-concurrent", vec![response]);
            let address = running.address();
            let body = Arc::new(target.body("Run once", "c2 concurrent"));
            let key = "c2-concurrent-000000001";
            let barrier = Arc::new(std::sync::Barrier::new(8));
            let clients = (0..8)
                .map(|_| {
                    let (body, barrier) = (body.clone(), barrier.clone());
                    std::thread::spawn(move || {
                        barrier.wait();
                        submit(address, key, &body)
                    })
                })
                .collect::<Vec<_>>();
            let answers = clients
                .into_iter()
                .map(|client| client.join().expect("joined"))
                .collect::<Vec<_>>();
            let jobs = answers.iter().map(Answer::job).collect::<BTreeSet<_>>();
            assert_eq!(jobs.len(), 1, "every acknowledgment names one Job");
            let created = answers.iter().filter(|answer| answer.status == 201).count();
            assert_eq!(created, 1, "one request created it");
            assert!(
                answers
                    .iter()
                    .all(|answer| matches!(answer.status, 200 | 201))
            );
            let [record] = jobs_named(&handle, "c2 concurrent")
                .try_into()
                .expect("one Job");
            assert!(jobs.contains(&record.id));

            open.send(()).expect("let the Provider answer");
            until(|| state(address, record.id) == "completed");
            assert_eq!(target.requests(), 1, "one execution");
            assert_eq!(jobs_named(&handle, "c2 concurrent").len(), 1);
            running.stop();
            assert_relationships_consistent(&directory);
        }

        #[test]
        fn a_key_reused_for_another_request_is_refused_without_a_job() {
            let (_root, directory) = data_directory("c2-conflict");
            let running = Running::start(&directory, LONG);
            let handle = running.handle.clone();
            let (open, response) = gated(answering("first", (3, 1)));
            let target = target(&handle, "c2-conflict", vec![response]);
            let address = running.address();
            let key = "c2-conflict-0000000001";
            let job = submit(address, key, &target.body("First", "c2 conflict")).job();
            // While it runs, and once it has settled, another request with
            // the key is refused, and nothing more is created.
            for different in [
                target.body("Second", "c2 conflict"),
                target.body("First", "c2 conflict renamed"),
            ] {
                let refused = submit(address, key, &different);
                assert_eq!(
                    (refused.status, refused.error().as_str()),
                    (409, "idempotency_key_conflict")
                );
            }
            open.send(()).expect("let the Provider answer");
            until(|| state(address, job) == "completed");
            let refused = submit(address, key, &target.body("Second", "c2 conflict"));
            assert_eq!(refused.status, 409);
            assert_eq!(
                lookup(address, key).admission(),
                ("admitted".to_owned(), Some(job.to_string()))
            );
            assert_eq!(jobs_named(&handle, "c2 conflict").len(), 1);
            assert!(jobs_named(&handle, "c2 conflict renamed").is_empty());
            assert_eq!(target.requests(), 1);
            running.stop();
        }

        /// Runs the real host with the listener, runs `client` on a thread
        /// of its own against its address, and stops the host.
        fn hosted<T: Send + 'static>(
            directory: &DataDirectory,
            admission_wait: Duration,
            client: impl FnOnce(SocketAddr) -> T + Send + 'static,
        ) -> T {
            let returned = Arc::new(std::sync::Mutex::new(None));
            let slot = returned.clone();
            let report = std::thread::scope(|scope| {
                scope
                    .spawn(move || {
                        run_controlled_reporting(
                            directory,
                            false,
                            Some(config(admission_wait)),
                            move |context: HostContext| {
                                let address = context.control_plane().expect("listening");
                                async move {
                                    let (returned, response) = flume::bounded(1);
                                    std::thread::spawn(move || returned.send(client(address)));
                                    let value = response
                                        .recv_async()
                                        .await
                                        .expect("the client did not panic");
                                    *slot.lock().expect("lock") = Some(value);
                                }
                            },
                        )
                    })
                    .join()
                    .expect("the host did not panic")
            });
            assert!(
                report
                    .failures
                    .iter()
                    .all(|failure| failure.phase == Phase::Logger),
                "{:?}",
                report.failures
            );
            returned
                .lock()
                .expect("lock")
                .take()
                .expect("the client ran")
        }

        #[test]
        fn admissions_survive_a_restart_and_are_never_executed_again() {
            let (_root, directory) = data_directory("c2-restart");
            let target = {
                let policy = crate::system_policy::SystemPolicy::load_for_startup(&directory)
                    .expect("policy");
                let worker =
                    DatabaseWorker::start_with_policy(&directory, policy).expect("start database");
                let target = target(
                    &worker.handle(),
                    "c2-restart",
                    vec![
                        now(answering("done", (2, 1))),
                        stalled(),
                        now(answering("queued done", (2, 1))),
                    ],
                );
                worker.shutdown().expect("shutdown");
                target
            };
            let done_body = target.body("Finish", "c2 restart done");
            let running_body = target.body("Keep running", "c2 restart running");
            let queued_body = target.body("Wait in line", "c2 restart queued");
            let (done_key, running_key, queued_key) = (
                "c2-restart-done-000001",
                "c2-restart-running-0001",
                "c2-restart-queued-00001",
            );
            let bodies = (done_body.clone(), running_body.clone(), queued_body.clone());
            let requests = target.requests.clone();
            let (done, left, queued_id) = hosted(&directory, SHORT, move |address| {
                let (done_body, running_body, queued_body) = bodies;
                let done = admitted(address, done_key, &done_body);
                until(|| state(address, done) == "completed");
                let left = admitted(address, running_key, &running_body);
                until(|| requests.load(Ordering::SeqCst) == 2);
                // The executor is busy, so this one is only queued when the
                // host stops: it is pending, never admitted.
                let queued = submit(address, queued_key, &queued_body);
                assert_eq!(queued.status, 201, "{}", queued.body);
                assert_eq!(lookup(address, queued_key).admission().0, "admitted");
                (done, left, queued.job())
            });
            assert_eq!(target.requests(), 2);

            // A new process: the database, not any memory of the earlier one,
            // answers; nothing of it runs again.
            let requests = target.requests.clone();
            let (done_again, queued) = hosted(&directory, LONG, move |address| {
                assert_eq!(
                    lookup(address, done_key).admission(),
                    ("admitted".to_owned(), Some(done.to_string()))
                );
                assert_eq!(
                    lookup(address, running_key).admission(),
                    ("admitted".to_owned(), Some(left.to_string()))
                );
                // The shutdown stopped it after its request was delivered.
                assert_eq!(state(address, left), "unknown");
                assert_eq!(stop(address, left).stop(), "already_settled");
                let replay = submit(address, running_key, &running_body);
                assert_eq!((replay.status, replay.job()), (200, left));
                let replay = submit(address, done_key, &done_body);
                assert_eq!((replay.status, replay.job()), (200, done));
                let conflict = submit(address, done_key, &queued_body);
                assert_eq!(conflict.status, 409);
                // The queued submission never created a Job, and the restart
                // settled that: its key is free and creates one now.
                assert_eq!(
                    lookup(address, queued_key).admission(),
                    ("admitted".into(), Some(queued_id.to_string()))
                );
                let queued = submit(address, queued_key, &queued_body);
                assert_eq!((queued.status, queued.job()), (200, queued_id));
                assert_eq!(state(address, queued_id), "cancelled");
                assert_eq!(requests.load(Ordering::SeqCst), 2);
                (done, queued_id)
            });
            assert_eq!(done_again, done);
            let worker = DatabaseWorker::start(&directory).expect("reopen");
            let handle = worker.handle();
            assert_eq!(jobs_named(&handle, "c2 restart done").len(), 1);
            assert_eq!(jobs_named(&handle, "c2 restart running").len(), 1);
            let [record] = jobs_named(&handle, "c2 restart queued")
                .try_into()
                .expect("one Job, created after the restart");
            assert_eq!(record.id, queued);
            let left = job_record(&handle, left);
            assert!(left.unknown_at.is_some() && left.cancelled_at.is_none());
            worker.shutdown().expect("shutdown");
            assert_eq!(target.requests(), 2, "nothing ran twice");
            assert_relationships_consistent(&directory);
        }

        #[test]
        fn a_lost_acknowledgment_is_reconciled_without_a_second_job() {
            let (_root, directory) = data_directory("c2-lost");
            let running = Running::start(&directory, SHORT);
            let handle = running.handle.clone();
            let (open_busy, busy) = gated(answering("busy done", (2, 1)));
            let target = target(
                &handle,
                "c2-lost",
                vec![
                    busy,
                    now(answering("before done", (2, 1))),
                    now(answering("after done", (2, 1))),
                ],
            );
            let address = running.address();

            // The executor is busy, so the next admission cannot end yet.
            let busy = admitted(
                address,
                "c2-lost-busy-000000001",
                &target.body("Busy", "c2 busy"),
            );
            until(|| target.requests() == 1);

            // Lost before the admission ended: the client sends the whole
            // request, the admission starts, and the connection is gone.
            let before_key = "c2-lost-before-0000001";
            let before_body = target.body("Before", "c2 lost before");
            let connection = submission(address, before_key, &before_body);
            until(|| {
                matches!(
                    lookup(address, before_key).admission().0.as_str(),
                    "pending" | "admitted"
                )
            });
            drop(connection);
            // A retry while it is pending waits on the same admission.
            let retry = submit(address, before_key, &before_body);
            assert_eq!(
                (retry.status, retry.admission().0.as_str()),
                (200, "admitted")
            );
            assert_eq!(jobs_named(&handle, "c2 lost before").len(), 1);
            open_busy.send(()).expect("free the executor");
            until(|| lookup(address, before_key).admission().0 == "admitted");
            let before = lookup(address, before_key).job();
            until(|| state(address, before) == "completed");

            // Lost after the Job was created: the client never reads the
            // answer, and the connection is gone once the Job is durable.
            let after_key = "c2-lost-after-00000001";
            let after_body = target.body("After", "c2 lost after");
            let connection = submission(address, after_key, &after_body);
            until(|| jobs_named(&handle, "c2 lost after").len() == 1);
            drop(connection);
            let after = lookup(address, after_key).job();
            let replay = submit(address, after_key, &after_body);
            assert_eq!((replay.status, replay.job()), (200, after));
            let replay = submit(address, before_key, &before_body);
            assert_eq!((replay.status, replay.job()), (200, before));
            until(|| state(address, after) == "completed");

            assert_eq!(jobs_named(&handle, "c2 lost before").len(), 1);
            assert_eq!(jobs_named(&handle, "c2 lost after").len(), 1);
            assert_eq!(target.requests(), 3, "each Job ran once");
            assert_eq!(state(address, busy), "completed");
            until(|| running.control.held() == (0, 0));
            running.stop();
            assert_relationships_consistent(&directory);
        }

        #[test]
        fn an_unknown_commit_stays_uncertain_and_is_never_executed_again() {
            let (_root, directory) = data_directory("c2-unknown");
            let running = Running::start(&directory, LONG);
            let handle = running.handle.clone();
            let target = target(&handle, "c2-unknown", vec![now(answering("later", (2, 1)))]);
            let companion = create_job(
                &handle,
                NewJob {
                    eligible_at: Some(Timestamp::from_unix_micros(
                        Timestamp::now().as_unix_micros() + 3_600_000_000,
                    )),
                    configuration: llm_only(target.llm),
                    ..new_job(&handle, "companion", handle.genesis(), Vec::new())
                },
            );
            let failed_group = Arc::new(std::sync::Mutex::new(None));
            let (group, seam_handle) = (failed_group.clone(), handle.clone());
            provider_seam::before_creation_of("c2 unknown", move || {
                let mut group = group.lock().expect("lock");
                if group.is_none() {
                    *group = Some(fail_next_commit(&seam_handle, companion));
                }
            });
            let address = running.address();
            let key = "c2-unknown-commit-00001";
            let body = target.body("Uncertain", "c2 unknown");

            let answer = submit(address, key, &body);
            assert_eq!(
                (answer.status, answer.error().as_str()),
                (500, "admission_uncertain"),
                "an unknown commit is not a refusal"
            );
            let companion = failed_group
                .lock()
                .expect("lock")
                .take()
                .expect("the commit was failed");
            assert!(companion.join().expect("joined").is_err());
            // Neither a lookup nor a retry treats it as refused, and the
            // retry submits nothing.
            assert_eq!(
                lookup(address, key).admission(),
                ("uncertain".to_owned(), None)
            );
            let retry = submit(address, key, &body);
            assert_eq!(retry.error(), "admission_uncertain");
            let conflict = submit(address, key, &target.body("Other", "c2 unknown"));
            assert_eq!(conflict.error(), "idempotency_key_conflict");
            assert_eq!(running.control.held(), (0, 1));
            assert_eq!(target.requests(), 0, "nothing was sent");

            // Once storage is recovered, the database says what happened.
            wait(handle.recover_storage()).expect("recover storage");
            let (status, job) = lookup(address, key).admission();
            let jobs = jobs_named(&handle, "c2 unknown");
            match status.as_str() {
                "admitted" => {
                    let [record] = jobs.try_into().expect("the one Job");
                    assert_eq!(job, Some(record.id.to_string()));
                    // The recovery reopened the file, which settled the claim of
                    // the lost opening as unknown; nothing executes it again.
                    let replay = submit(address, key, &body);
                    assert_eq!((replay.status, replay.job()), (200, record.id));
                    until(|| state(address, record.id) == "cancelled");
                    assert_eq!(target.requests(), 0);
                }
                "not_found" => {
                    assert!(jobs.is_empty(), "no Job: {jobs:?}");
                    let created = submit(address, key, &body);
                    assert_eq!(created.status, 201, "{}", created.body);
                    let job = created.job();
                    until(|| state(address, job) == "completed");
                    assert_eq!(target.requests(), 1);
                }
                other => panic!("recovery resolves the key, got {other}"),
            }
            assert_eq!(running.control.held().1, 0);
            assert!(jobs_named(&handle, "c2 unknown").len() <= 1);
            running.stop();
        }

        #[test]
        fn a_stop_reaches_only_the_job_it_names() {
            let (_root, directory) = data_directory("c2-stop");
            let running = Running::start(&directory, SHORT);
            let handle = running.handle.clone();
            let target = target(
                &handle,
                "c2-stop",
                vec![stalled(), now(answering("second answer", (3, 1)))],
            );
            let address = running.address();
            let first = admitted(
                address,
                "c2-stop-first-00000001",
                &target.body("One", "c2 stopped"),
            );
            until(|| target.requests() == 1);
            // The second shares the Profile and Provider, and waits for the
            // executor.
            let second_key = "c2-stop-second-0000001";
            let pending = submit(address, second_key, &target.body("Two", "c2 untouched"));
            assert_eq!(pending.admission().0, "admitted");

            let stopped = stop(address, first);
            assert_eq!(
                (stopped.status, stopped.stop().as_str()),
                (202, "requested")
            );
            assert_eq!(stopped.json()["stop"]["job"], first.to_string());
            until(|| job_record(&handle, first).stop_confirmed_at.is_some());
            let record = job_record(&handle, first);
            assert!(
                record.unknown_at.is_some()
                    && record.stop_confirmed_at.is_some()
                    && record.cancelled_at.is_none(),
                "a stopped request that reached the Provider is Unknown, not Cancelled"
            );
            until(|| lookup(address, second_key).admission().0 == "admitted");
            let second = lookup(address, second_key).job();
            assert_ne!(second, first);
            until(|| state(address, second) != "unsettled");
            assert_eq!(
                state(address, second),
                "completed",
                "the sibling was not stopped"
            );
            assert_eq!(
                (stop(address, first).stop(), stop(address, second).stop()),
                ("already_settled".to_owned(), "already_settled".to_owned())
            );
            // A Job this listener never admitted has nothing here to stop,
            // and an unknown one, or the root of the tree, is not found.
            let foreign = create_job(
                &handle,
                NewJob {
                    eligible_at: Some(Timestamp::from_unix_micros(
                        Timestamp::now().as_unix_micros() + 3_600_000_000,
                    )),
                    configuration: llm_only(target.llm),
                    ..new_job(&handle, "foreign pending", handle.genesis(), Vec::new())
                },
            );
            let refused = stop(address, foreign);
            assert_eq!(
                (refused.status, refused.error().as_str()),
                (409, "not_stoppable")
            );
            assert!(!settled(&job_record(&handle, foreign)));
            for job in [JobId::new(999_999), handle.genesis()] {
                let missing = stop(address, job);
                assert_eq!(
                    (missing.status, missing.error().as_str()),
                    (404, "job_not_found")
                );
            }
            until(|| running.control.held() == (0, 0));
            running.stop();
            assert_relationships_consistent(&directory);
        }

        #[test]
        fn a_stop_racing_settlement_or_shutdown_claims_no_outcome() {
            let (_root, directory) = data_directory("c2-races");
            let running = Running::start(&directory, LONG);
            let handle = running.handle.clone();
            let mut gates = Vec::new();
            let mut responses = Vec::new();
            for round in 0..6 {
                let (open, response) = gated(answering(&format!("answer {round}"), (2, 1)));
                gates.push(open);
                responses.push(response);
            }
            responses.push(stalled());
            let target = target(&handle, "c2-races", responses);
            let address = running.address();
            // Stop against settlement: either may win; neither invents the
            // other's result.
            for (round, open) in gates.into_iter().enumerate() {
                let key = format!("c2-race-settle-{round:08}");
                let job = submit(
                    address,
                    &key,
                    &target.body("Race", &format!("c2 race {round}")),
                )
                .job();
                until(|| target.requests() == round + 1);
                let stopping = std::thread::spawn(move || stop(address, job));
                open.send(()).ok();
                let answer = stopping.join().expect("joined");
                let reported = answer.stop();
                until(|| state(address, job) != "unsettled");
                let record = job_record(&handle, job);
                assert!(record.cancelled_at.is_none(), "a stop is never Cancelled");
                assert!(record.completed_at.is_some() || record.unknown_at.is_some());
                match reported.as_str() {
                    "requested" => assert_eq!(answer.status, 202),
                    "already_settled" => assert_eq!(answer.status, 200),
                    other => panic!("unexpected stop answer {other}"),
                }
                // Stopping again changes nothing.
                assert_eq!(stop(address, job).stop(), "already_settled");
            }
            until(|| running.control.held() == (0, 0));

            // Stop against shutdown: the listener stops first; whichever
            // reaches the execution, it settles once, as a stop does.
            let last = submit(
                address,
                "c2-race-shutdown-00001",
                &target.body("Last", "c2 last"),
            )
            .job();
            until(|| target.requests() == 7);
            let stopping = std::thread::spawn(move || {
                std::panic::catch_unwind(|| stop(address, last).stop()).ok()
            });
            let Running {
                worker,
                handle,
                runtime,
                control,
            } = running;
            wait(control.stop()).expect("stop the listener");
            wait(runtime.shutdown()).expect("shutdown runtime");
            if let Some(answer) = stopping.join().expect("joined") {
                assert!(matches!(answer.as_str(), "requested" | "already_settled"));
            }
            let record = job_record(&handle, last);
            assert!(
                record.unknown_at.is_some()
                    && record.stop_confirmed_at.is_some()
                    && record.cancelled_at.is_none()
            );
            worker.shutdown().expect("shutdown");

            // A later process has no stop of it, and says it is settled.
            let running = Running::start(&directory, LONG);
            assert_eq!(stop(running.address(), last).stop(), "already_settled");
            assert_eq!(running.control.held(), (0, 0));
            running.stop();
            assert_relationships_consistent(&directory);
        }

        #[test]
        fn submissions_and_stops_are_refused_unless_authentic_well_formed_and_runnable() {
            let (_root, directory) = data_directory("c2-security");
            let running = Running::start(&directory, LONG);
            let handle = running.handle.clone();
            let target = target(&handle, "c2-security", vec![now(answering("ok", (2, 1)))]);
            let system = executor(&handle, "system worker", &[]).expect("register");
            let agent = agent_profile(&handle, "Be brief.", vec![target.llm]);
            let address = running.address();
            let port = address.port();
            let host = host(address);
            let bearer = bearer();
            let body = target.body("Secure", "c2 secure");
            let length = body.len().to_string();
            let key = "c2-security-key-000001";
            let submit_line = "POST /api/v1/jobs";
            let before = wait(handle.jobs(None, 1_000)).expect("read").len();
            let mut answers = Vec::new();
            let mut refused =
                |status: u16, code: &str, line: &str, headers: &[(&str, &str)], body: &[u8]| {
                    let answer = send(address, line, headers, body);
                    assert_eq!(
                        (answer.status, answer.error().as_str()),
                        (status, code),
                        "{line} {headers:?}"
                    );
                    answers.push(answer);
                };
            let json = ("Content-Type", "application/json");
            let standard = vec![
                ("Host", host.as_str()),
                ("Authorization", bearer.as_str()),
                ("Idempotency-Key", key),
                json,
                ("Content-Length", length.as_str()),
            ];
            // Authentication, Host and Origin, on every new route.
            let stop_line = format!("POST /api/v1/jobs/{}/stop", handle.genesis());
            for (line, sent) in [
                (submit_line, body.as_bytes()),
                ("GET /api/v1/admissions", b"".as_slice()),
                (stop_line.as_str(), b"".as_slice()),
            ] {
                let length = sent.len().to_string();
                let mut common = vec![
                    ("Host", host.as_str()),
                    ("Authorization", bearer.as_str()),
                    ("Idempotency-Key", key),
                ];
                if !sent.is_empty() {
                    common.extend([json, ("Content-Length", length.as_str())]);
                }
                let mut unauthenticated = replaced(&common, &[]);
                unauthenticated.retain(|(name, _)| *name != "Authorization");
                refused(401, "unauthenticated", line, &unauthenticated, sent);
                refused(
                    401,
                    "unauthenticated",
                    line,
                    &replaced(
                        &common,
                        &[("Authorization", "Bearer not-the-token-0123456789abcdef")],
                    ),
                    sent,
                );
                let duplicated = [
                    replaced(&common, &[]),
                    vec![("Authorization", bearer.as_str())],
                ]
                .concat();
                refused(401, "unauthenticated", line, &duplicated, sent);
                refused(
                    403,
                    "forbidden_host",
                    line,
                    &replaced(&common, &[("Host", "evil.example")]),
                    sent,
                );
                refused(
                    403,
                    "forbidden_origin",
                    line,
                    &replaced(&common, &[("Origin", "http://evil.example")]),
                    sent,
                );
                let other = format!("http://localhost:{port}");
                refused(
                    403,
                    "forbidden_origin",
                    line,
                    &replaced(&common, &[("Origin", &other)]),
                    sent,
                );
                refused(
                    403,
                    "forbidden_origin",
                    line,
                    &replaced(&common, &[("Sec-Fetch-Site", "cross-site")]),
                    sent,
                );
            }
            // Methods: POST only where it submits or stops; GET elsewhere.
            let only_post = send(
                address,
                "PUT /api/v1/jobs",
                &replaced(&standard, &[]),
                body.as_bytes(),
            );
            assert_eq!(
                (
                    only_post.status,
                    only_post.headers.get("allow").map(String::as_str)
                ),
                (405, Some("POST"))
            );
            let only_post = send(
                address,
                &format!("DELETE {}", &stop_line[5..]),
                &replaced(&standard, &[]),
                b"",
            );
            assert_eq!(only_post.status, 405);
            for line in [
                "POST /api/v1/admissions",
                "POST /api/v1/health",
                "POST /api/v1/jobs/job:1",
            ] {
                let answer = send(address, line, &replaced(&standard, &[]), body.as_bytes());
                assert_eq!(
                    (
                        answer.status,
                        answer.headers.get("allow").map(String::as_str)
                    ),
                    (405, Some("GET")),
                    "{line}"
                );
            }
            // Keys travel in their header only, once, well formed.
            let mut keyless = replaced(&standard, &[]);
            keyless.retain(|(name, _)| *name != "Idempotency-Key");
            refused(
                400,
                "idempotency_key_required",
                submit_line,
                &keyless,
                body.as_bytes(),
            );
            let twice = [replaced(&standard, &[]), vec![("Idempotency-Key", key)]].concat();
            refused(
                400,
                "invalid_idempotency_key",
                submit_line,
                &twice,
                body.as_bytes(),
            );
            let long_key = "k".repeat(129);
            for malformed in ["short", "has space in-the-key-0001", &long_key] {
                refused(
                    400,
                    "invalid_idempotency_key",
                    submit_line,
                    &replaced(&standard, &[("Idempotency-Key", malformed)]),
                    body.as_bytes(),
                );
            }
            refused(
                400,
                "invalid_request",
                &format!("GET /api/v1/admissions?idempotency_key={key}"),
                &[("Host", &host), ("Authorization", &bearer)],
                b"",
            );
            let mut keyless_lookup =
                vec![("Host", host.as_str()), ("Authorization", bearer.as_str())];
            refused(
                400,
                "idempotency_key_required",
                "GET /api/v1/admissions",
                &keyless_lookup,
                b"",
            );
            keyless_lookup.extend([("Idempotency-Key", key), ("Idempotency-Key", key)]);
            refused(
                400,
                "invalid_idempotency_key",
                "GET /api/v1/admissions",
                &keyless_lookup,
                b"",
            );
            refused(
                400,
                "invalid_request",
                &format!("GET /api/v1/admissions?key={key}"),
                &[
                    ("Host", &host),
                    ("Authorization", &bearer),
                    ("Idempotency-Key", key),
                ],
                b"",
            );
            refused(
                400,
                "invalid_request",
                "POST /api/v1/jobs?x=1",
                &replaced(&standard, &[]),
                body.as_bytes(),
            );
            // Bodies: declared JSON, of one bounded length, read strictly.
            refused(
                415,
                "unsupported_media_type",
                submit_line,
                &replaced(&standard, &[("Content-Type", "text/plain")]),
                body.as_bytes(),
            );
            let mut untyped = replaced(&standard, &[]);
            untyped.retain(|(name, _)| *name != "Content-Type");
            refused(
                415,
                "unsupported_media_type",
                submit_line,
                &untyped,
                body.as_bytes(),
            );
            let too_long = (MAX_SUBMISSION_BYTES + 1).to_string();
            refused(
                413,
                "body_too_large",
                submit_line,
                &replaced(&standard, &[("Content-Length", &too_long)]),
                b"",
            );
            let mut chunked = replaced(&standard, &[("Transfer-Encoding", "chunked")]);
            chunked.retain(|(name, _)| *name != "Content-Length");
            let answer = send(address, submit_line, &chunked, b"0\r\n\r\n");
            assert_eq!(answer.status, 411, "{}", answer.body);
            for malformed in [
                "{".to_owned(),
                "[]".to_owned(),
                json!({"llm_profile": target.llm.to_string(), "executor": target.executor.to_string(), "objective": "x", "credential_ref": "KEY"}).to_string(),
                json!({"llm_profile": target.llm.to_string(), "executor": target.executor.to_string(), "objective": "x", "endpoint": "https://evil.example/v1"}).to_string(),
                json!({"llm_profile": target.llm.to_string(), "executor": target.executor.to_string(), "objective": "x", "command": "rm -rf /"}).to_string(),
                json!({"llm_profile": target.llm.to_string(), "executor": target.executor.to_string(), "objective": "x", "job": "job:1"}).to_string(),
                json!({"llm_profile": target.llm.to_string(), "executor": target.executor.to_string()}).to_string(),
            ] {
                let length = malformed.len().to_string();
                refused(
                    400,
                    "invalid_body",
                    submit_line,
                    &replaced(&standard, &[("Content-Length", &length)]),
                    malformed.as_bytes(),
                );
            }
            // A stop carries no body.
            refused(
                413,
                "body_not_accepted",
                &stop_line,
                &[
                    ("Host", &host),
                    ("Authorization", &bearer),
                    ("Content-Length", "2"),
                ],
                b"{}",
            );
            // The C1 routes are as they were.
            assert_eq!(get(address, "/api/v1/jobs").error(), "not_found");
            assert_eq!(
                get(address, &format!("/api/v1/jobs/{}/stop", handle.genesis())).error(),
                "not_found"
            );

            // Configurations that cannot run are refused, and the key stays
            // free for one that can.
            for (llm, executor, code) in [
                (
                    ProfileId::new(999_999),
                    target.executor,
                    "llm_profile_not_found",
                ),
                (agent, target.executor, "llm_profile_not_found"),
                (target.llm, ExecutorId::new(999_999), "executor_not_found"),
                (target.llm, system, "executor_incompatible"),
            ] {
                let body = json!({
                    "llm_profile": llm.to_string(),
                    "executor": executor.to_string(),
                    "objective": "Secure",
                    "name": "c2 secure",
                })
                .to_string();
                let answer = submit(address, key, &body);
                assert_eq!((answer.status, answer.error().as_str()), (422, code));
                assert_eq!(lookup(address, key).admission().0, "not_found");
            }
            assert_eq!(wait(handle.jobs(None, 1_000)).expect("read").len(), before);
            assert_eq!(target.requests(), 0);
            for answer in &answers {
                assert!(!answer.body.contains(TOKEN) && !answer.body.contains(key));
                assert!(
                    !answer
                        .headers
                        .keys()
                        .any(|name| name.starts_with("access-control-"))
                );
                assert!(!answer.headers.contains_key("set-cookie"));
            }

            // Storage that refuses commands admits nothing, and says so.
            let accepted = submit(address, key, &body);
            assert_eq!(accepted.status, 201, "{}", accepted.body);
            until(|| state(address, accepted.job()) == "completed");
            protect(&handle, directory.database_path());
            let protected_key = "c2-security-protected1";
            let refused = submit(
                address,
                protected_key,
                &target.body("Protected", "c2 protected"),
            );
            assert_eq!(
                (refused.status, refused.error().as_str()),
                (503, "storage_refused")
            );
            assert_eq!(lookup(address, protected_key).admission().0, "not_found");
            assert!(jobs_named(&handle, "c2 protected").is_empty());
            running.stop();
        }

        /// Submits on a thread of its own, so the test can go on while the
        /// request waits.
        fn submitting(
            address: SocketAddr,
            key: &'static str,
            body: String,
        ) -> std::thread::JoinHandle<Answer> {
            std::thread::spawn(move || submit(address, key, &body))
        }

        #[test]
        fn a_conflicting_replay_never_displaces_a_committed_key() {
            let (_root, directory) = data_directory("c2f1-committed");
            let running = Running::start(&directory, LONG);
            let handle = running.handle.clone();
            let target = target(
                &handle,
                "c2f1-committed",
                vec![now(answering("bound", (2, 1)))],
            );
            let address = running.address();
            let key = "c2f1-committed-0000001";
            let body = target.body("Bind it", "c2f1 committed");
            let created = submit(address, key, &body);
            assert_eq!(created.status, 201, "{}", created.body);
            let job = created.job();
            until(|| state(address, job) == "completed");
            until(|| running.control.held() == (0, 0));
            let admitted = ("admitted".to_owned(), Some(job.to_string()));

            // A conflicting replay is held once it has read the record and
            // before anything is decided from it.
            let (reached, release) = resolution_seam::hold(key);
            let conflicting = submitting(address, key, target.body("Other", "c2f1 committed"));
            reached
                .recv_timeout(crate::test_support::WAIT)
                .expect("the conflicting replay reads the record");
            assert_eq!(running.control.held(), (0, 1), "it reserves the key");
            assert_eq!(lookup(address, key).admission(), admitted);
            // A valid replay meanwhile waits on that resolution; it is not
            // refused by the conflicting request.
            let valid = submitting(address, key, body.clone());
            until(|| running.control.waiting(key) == 2);
            assert_eq!(lookup(address, key).admission(), admitted);
            release.send(()).expect("release the conflicting replay");

            let conflicting = conflicting.join().expect("joined");
            assert_eq!(
                (conflicting.status, conflicting.error().as_str()),
                (409, "idempotency_key_conflict")
            );
            let valid = valid.join().expect("joined");
            assert_eq!((valid.status, valid.job()), (200, job));
            assert_eq!(lookup(address, key).admission(), admitted);
            assert_eq!(jobs_named(&handle, "c2f1 committed").len(), 1);
            assert_eq!(target.requests(), 1, "one Provider execution");
            assert_eq!(running.control.held(), (0, 0), "nothing is left held");
            running.stop();
            assert_relationships_consistent(&directory);
        }

        #[test]
        fn the_first_claim_of_a_key_owns_it_until_its_record_is_read() {
            let (_root, directory) = data_directory("c2f1-claim");
            let running = Running::start(&directory, LONG);
            let handle = running.handle.clone();
            let target = target(&handle, "c2f1-claim", vec![now(answering("first", (2, 1)))]);
            let address = running.address();
            let key = "c2f1-claim-0000000001";
            let first = target.body("First", "c2f1 first");
            let second = target.body("Second", "c2f1 second");

            // The first request has read that the key is not recorded.
            let (reached, release) = resolution_seam::hold(key);
            let owner = submitting(address, key, first.clone());
            reached
                .recv_timeout(crate::test_support::WAIT)
                .expect("the first request reads the record");
            // Another request, of either request, cannot bind the key
            // meanwhile: it waits on that read instead of admitting.
            let other = submitting(address, key, second.clone());
            let same = submitting(address, key, first.clone());
            until(|| running.control.waiting(key) == 3);
            assert_eq!(
                lookup(address, key).admission(),
                ("pending".to_owned(), None)
            );
            assert!(jobs_named(&handle, "c2f1 first").is_empty());
            assert!(jobs_named(&handle, "c2f1 second").is_empty());
            release.send(()).expect("release the first request");

            let owner = owner.join().expect("joined");
            assert_eq!(owner.status, 201, "{}", owner.body);
            let job = owner.job();
            let same = same.join().expect("joined");
            assert_eq!((same.status, same.job()), (200, job));
            let other = other.join().expect("joined");
            assert_eq!(other.error(), "idempotency_key_conflict");
            until(|| state(address, job) == "completed");
            assert_eq!(
                lookup(address, key).admission(),
                ("admitted".to_owned(), Some(job.to_string()))
            );
            assert_eq!(jobs_named(&handle, "c2f1 first").len(), 1);
            assert!(jobs_named(&handle, "c2f1 second").is_empty());
            assert_eq!(target.requests(), 1);
            until(|| running.control.held() == (0, 0));
            running.stop();
            assert_relationships_consistent(&directory);
        }

        #[test]
        fn a_binding_made_past_the_listener_during_a_read_stays_authoritative() {
            use crate::persistence::{NewSubmission, RequestFingerprint};

            let (_root, directory) = data_directory("c2f1-stale");
            let running = Running::start(&directory, LONG);
            let handle = running.handle.clone();
            let target = target(&handle, "c2f1-stale", Vec::new());
            let address = running.address();
            let key = "c2f1-stale-read-000001";
            let body = target.body("Stale", "c2f1 stale");

            // The request reads a miss and is held; another writer, past
            // this listener, binds the key before it goes on.
            let (reached, release) = resolution_seam::hold(key);
            let stale = submitting(address, key, body.clone());
            reached
                .recv_timeout(crate::test_support::WAIT)
                .expect("the request reads the record");
            let bound = wait(handle.create_submitted_job(
                NewJob {
                    eligible_at: Some(Timestamp::from_unix_micros(
                        Timestamp::now().as_unix_micros() + 3_600_000_000,
                    )),
                    input: JobInputDraft {
                        objective: Some("bound elsewhere".into()),
                        ..JobInputDraft::default()
                    },
                    ..NewJob {
                        configuration: llm_only(target.llm),
                        ..new_job(
                            &handle,
                            "c2f1 bound elsewhere",
                            handle.genesis(),
                            Vec::new(),
                        )
                    }
                },
                NewSubmission {
                    key: resolution_seam::key(key),
                    fingerprint: RequestFingerprint::from_digest([9; 32]),
                },
            ))
            .expect("bound elsewhere");
            release.send(()).expect("release the request");

            // Its stale read installs nothing over the binding: the creation
            // refuses the key, and the binding answers from then on.
            let stale = stale.join().expect("joined");
            assert_eq!(stale.error(), "idempotency_key_conflict");
            assert_eq!(
                lookup(address, key).admission(),
                ("admitted".to_owned(), Some(bound.to_string()))
            );
            assert_eq!(
                submit(address, key, &body).error(),
                "idempotency_key_conflict"
            );
            assert!(jobs_named(&handle, "c2f1 stale").is_empty());
            assert_eq!(target.requests(), 0, "nothing ran");
            assert_eq!(running.control.held(), (0, 0));
            running.stop();
        }

        #[test]
        fn a_refused_resolution_releases_its_key_and_every_waiter() {
            let (_root, directory) = data_directory("c2f1-refused");
            let running = Running::start(&directory, LONG);
            let handle = running.handle.clone();
            let target = target(
                &handle,
                "c2f1-refused",
                vec![now(answering("later", (2, 1)))],
            );
            let address = running.address();
            let key = "c2f1-refused-00000001";
            let unrunnable = json!({
                "llm_profile": target.llm.to_string(),
                "executor": "executor:999999",
                "objective": "Never",
                "name": "c2f1 refused",
            })
            .to_string();
            let (reached, release) = resolution_seam::hold(key);
            let owner = submitting(address, key, unrunnable.clone());
            reached
                .recv_timeout(crate::test_support::WAIT)
                .expect("the request reads the record");
            let joined = submitting(address, key, unrunnable.clone());
            // A waiter that disconnects is not kept.
            let gone = submission(address, key, &unrunnable);
            until(|| running.control.waiting(key) == 3);
            drop(gone);
            until(|| running.control.waiting(key) == 2);
            release.send(()).expect("release the request");
            for answer in [
                owner.join().expect("joined"),
                joined.join().expect("joined"),
            ] {
                assert_eq!(
                    (answer.status, answer.error().as_str()),
                    (422, "executor_not_found")
                );
            }
            assert_eq!(running.control.held(), (0, 0), "the key is released");
            assert_eq!(
                lookup(address, key).admission(),
                ("not_found".to_owned(), None)
            );
            // The key is free: a runnable request is admitted once.
            let created = submit(address, key, &target.body("Later", "c2f1 refused"));
            assert_eq!(created.status, 201, "{}", created.body);
            let job = created.job();
            until(|| state(address, job) == "completed");
            until(|| running.control.held() == (0, 0));
            assert_eq!(jobs_named(&handle, "c2f1 refused").len(), 1);
            assert_eq!(target.requests(), 1);
            running.stop();
        }
    }

    // ---- Control Plane event streams --------------------------------------

    mod control_plane_events {
        use std::collections::BTreeMap;
        use std::io::{BufRead, BufReader, Read, Write};
        use std::net::{SocketAddr, TcpListener, TcpStream};
        use std::time::Instant;

        use super::*;
        use crate::control_plane::events_seam::{self, Point};
        use crate::control_plane::{ControlPlane, ControlPlaneConfig};
        use crate::orchestration::execution_runtime::provider_seam::{self, Response};
        use crate::persistence::{IntermediateOutputKind, NewJobOutput};

        const TOKEN: &str = "C3-Event-Stream-Token_0123456789.abcdefghij~";

        /// Limits small enough to observe: 4 streams, read every 10 ms,
        /// silent at most 100 ms, a 2 s stall and a 60 s lifetime.
        fn config() -> ControlPlaneConfig {
            limited(Duration::from_secs(2), Duration::from_secs(60))
        }

        fn limited(stall: Duration, lifetime: Duration) -> ControlPlaneConfig {
            ControlPlaneConfig::new(0, TOKEN)
                .expect("a valid configuration")
                .with_streams(
                    4,
                    Duration::from_millis(10),
                    Duration::from_millis(100),
                    stall,
                    lifetime,
                )
        }

        /// The real database, execution runtime and listener, started and
        /// stopped as the host starts and stops them.
        struct Running {
            worker: DatabaseWorker,
            runtime: ExecutionRuntime,
            handle: DatabaseHandle,
            control: ControlPlane,
        }

        impl Running {
            fn start(directory: &DataDirectory, config: ControlPlaneConfig) -> Self {
                let worker =
                    DatabaseWorker::start_with_config(directory, lenient()).expect("start");
                let handle = worker.handle();
                let runtime = ExecutionRuntime::start(handle.clone()).expect("start runtime");
                let scheduler = runtime.scheduler();
                let control =
                    ControlPlane::start(config, handle.clone(), scheduler).expect("start listener");
                Self {
                    worker,
                    runtime,
                    handle,
                    control,
                }
            }

            fn address(&self) -> SocketAddr {
                self.control.address()
            }

            fn open_streams(&self) -> usize {
                self.control.streams().0
            }

            fn stop(self) {
                wait(self.control.stop()).expect("stop the listener");
                wait(self.runtime.shutdown()).expect("shutdown runtime");
                self.worker.shutdown().expect("shutdown");
            }
        }

        /// One SSE event, or a comment.
        #[derive(Clone, Debug, Default, PartialEq)]
        struct Event {
            id: Option<String>,
            name: Option<String>,
            data: Option<String>,
            comment: Option<String>,
        }

        impl Event {
            fn json(&self) -> Value {
                serde_json::from_str(self.data.as_deref().expect("data"))
                    .unwrap_or_else(|_| panic!("JSON data: {self:?}"))
            }

            fn name(&self) -> &str {
                self.name.as_deref().unwrap_or("")
            }

            /// An output event's output, checked against its envelope.
            fn output(&self, job: JobId) -> Value {
                assert_eq!(self.name(), "output", "{self:?}");
                let data = self.json();
                assert_eq!(data["api_version"], 1);
                assert_eq!(data["job"], job.to_string());
                let output = data["output"].clone();
                assert_eq!(
                    self.id.as_deref(),
                    Some(output["seq"].to_string().as_str()),
                    "its id is its stored seq"
                );
                output
            }

            fn settlement(&self, job: JobId) -> Value {
                assert_eq!(self.name(), "settlement", "{self:?}");
                assert_eq!(self.id, None);
                let data = self.json();
                assert_eq!(data["job"], job.to_string());
                data
            }

            fn job(&self) -> Value {
                assert_eq!(self.name(), "job", "{self:?}");
                assert_eq!(self.id, None);
                self.json()["job"].clone()
            }
        }

        /// A response read as it arrives: an event stream, or the whole of
        /// any other answer.
        struct Stream {
            status: u16,
            headers: BTreeMap<String, String>,
            reader: BufReader<TcpStream>,
            /// Decoded bytes not yet parsed into events.
            text: Vec<u8>,
            /// Every decoded byte, for what must never be sent.
            raw: Vec<u8>,
            ended: bool,
        }

        fn stream_with(address: SocketAddr, line: &str, headers: &[(&str, &str)]) -> Stream {
            let connection = TcpStream::connect(address).expect("connect");
            connection
                .set_read_timeout(Some(crate::test_support::WAIT))
                .expect("timeout");
            let mut request = format!("{line} HTTP/1.1\r\n");
            for (name, value) in headers {
                request.push_str(&format!("{name}: {value}\r\n"));
            }
            request.push_str("Connection: close\r\n\r\n");
            (&connection).write_all(request.as_bytes()).expect("send");
            let mut reader = BufReader::new(connection);
            let mut status_line = String::new();
            reader.read_line(&mut status_line).expect("a status line");
            let status = status_line
                .split(' ')
                .nth(1)
                .and_then(|code| code.parse().ok())
                .unwrap_or_else(|| panic!("a status: {status_line:?}"));
            let mut headers = BTreeMap::new();
            loop {
                let mut line = String::new();
                reader.read_line(&mut line).expect("a header");
                let line = line.trim_end();
                if line.is_empty() {
                    break;
                }
                let (name, value) = line.split_once(": ").expect("a header");
                headers.insert(name.to_ascii_lowercase(), value.to_owned());
            }
            Stream {
                status,
                headers,
                reader,
                text: Vec::new(),
                raw: Vec::new(),
                ended: false,
            }
        }

        fn authorized(address: SocketAddr) -> (String, String) {
            (
                format!("127.0.0.1:{}", address.port()),
                format!("Bearer {TOKEN}"),
            )
        }

        /// A well-behaved client's stream of `job`'s events, after `last`
        /// by `Last-Event-ID` when given.
        fn events(address: SocketAddr, job: JobId, last: Option<i64>) -> Stream {
            let (host, bearer) = authorized(address);
            let last = last.map(|seq| seq.to_string());
            let mut headers = vec![("Host", host.as_str()), ("Authorization", bearer.as_str())];
            if let Some(last) = &last {
                headers.push(("Last-Event-ID", last));
            }
            let stream = stream_with(address, &format!("GET /api/v1/jobs/{job}/events"), &headers);
            assert_eq!(stream.status, 200, "{:?}", stream.headers);
            stream
        }

        /// The same, with the cursor in the query.
        fn events_after(address: SocketAddr, job: JobId, after_seq: i64) -> Stream {
            let (host, bearer) = authorized(address);
            let stream = stream_with(
                address,
                &format!("GET /api/v1/jobs/{job}/events?after_seq={after_seq}"),
                &[("Host", &host), ("Authorization", &bearer)],
            );
            assert_eq!(stream.status, 200, "{:?}", stream.headers);
            stream
        }

        impl Stream {
            /// Decodes the next chunk; false at the end of the body.
            fn chunk(&mut self) -> bool {
                if self.ended {
                    return false;
                }
                let mut size = Vec::new();
                if self
                    .reader
                    .read_until(b'\n', &mut size)
                    .expect("a chunk size")
                    == 0
                {
                    self.ended = true;
                    return false;
                }
                let size = std::str::from_utf8(&size)
                    .ok()
                    .and_then(|size| usize::from_str_radix(size.trim_end(), 16).ok())
                    .unwrap_or_else(|| {
                        panic!(
                            "a chunk size: {:?}",
                            String::from_utf8_lossy(&size[..size.len().min(200)])
                        )
                    });
                let mut chunk = vec![0; size + 2];
                self.reader.read_exact(&mut chunk).expect("a chunk");
                assert_eq!(&chunk[size..], b"\r\n");
                chunk.truncate(size);
                if size == 0 {
                    self.ended = true;
                    return false;
                }
                self.raw.extend_from_slice(&chunk);
                self.text.extend_from_slice(&chunk);
                true
            }

            /// The next event or comment, `None` at the end of the stream.
            fn next_any(&mut self) -> Option<Event> {
                loop {
                    if let Some(end) = self.text.windows(2).position(|pair| pair == b"\n\n") {
                        let block = self.text.drain(..end + 2).collect::<Vec<_>>();
                        let block = String::from_utf8(block).expect("UTF-8 events");
                        let mut event = Event::default();
                        for line in block.trim_end_matches('\n').split('\n') {
                            if let Some(comment) = line.strip_prefix(':') {
                                event.comment = Some(comment.trim().to_owned());
                                continue;
                            }
                            let (field, value) = line.split_once(": ").expect("a field");
                            let slot = match field {
                                "id" => &mut event.id,
                                "event" => &mut event.name,
                                "data" => &mut event.data,
                                _ => panic!("an unexpected field {field:?}"),
                            };
                            assert!(slot.is_none(), "{field} once per event");
                            *slot = Some(value.to_owned());
                        }
                        return Some(event);
                    }
                    if !self.chunk() {
                        assert!(self.text.is_empty(), "no partial event");
                        return None;
                    }
                }
            }

            /// The next event, past heartbeats; `None` at the end.
            fn next(&mut self) -> Option<Event> {
                loop {
                    let event = self.next_any()?;
                    if event.comment.is_none() {
                        return Some(event);
                    }
                    assert_eq!(event.comment.as_deref(), Some("heartbeat"));
                    assert_eq!((&event.id, &event.name, &event.data), (&None, &None, &None));
                }
            }

            fn event(&mut self) -> Event {
                self.next().expect("an event")
            }

            /// Every event left, to the end of the stream.
            fn rest(&mut self) -> Vec<Event> {
                std::iter::from_fn(|| self.next()).collect()
            }

            /// The body of a response that is not a stream.
            fn answer(mut self) -> Value {
                let mut body = Vec::new();
                self.reader.read_to_end(&mut body).expect("read");
                serde_json::from_slice(&body).expect("a JSON body")
            }

            fn error(self) -> (u16, String) {
                let status = self.status;
                let body = self.answer();
                (
                    status,
                    body["error"]["code"].as_str().expect("a code").to_owned(),
                )
            }
        }

        fn get(address: SocketAddr, path: &str) -> Value {
            let (host, bearer) = authorized(address);
            let response = stream_with(
                address,
                &format!("GET {path}"),
                &[("Host", &host), ("Authorization", &bearer)],
            );
            assert_eq!(response.status, 200, "{path}");
            response.answer()
        }

        /// The Job's outputs as the outputs route serves them.
        fn stored(address: SocketAddr, job: JobId) -> Vec<Value> {
            get(address, &format!("/api/v1/jobs/{job}/outputs?limit=100"))["outputs"]
                .as_array()
                .expect("outputs")
                .clone()
        }

        fn until(mut condition: impl FnMut() -> bool) {
            let deadline = Instant::now() + crate::test_support::WAIT;
            while !condition() {
                assert!(Instant::now() < deadline, "not reached in time");
                std::thread::sleep(Duration::from_millis(5));
            }
        }

        fn progress(handle: &DatabaseHandle, job: JobId, text: &str) -> i64 {
            wait(handle.append_job_output(
                job,
                NewJobOutput {
                    kind: IntermediateOutputKind::Progress,
                    summary: Some(text.to_owned()),
                    code: None,
                    goal_reached: None,
                    value: None,
                },
            ))
            .expect("append an output")
            .seq
        }

        /// A Job this test runs by hand: started locally, no Provider.
        fn running_job(handle: &DatabaseHandle) -> JobId {
            let (_provider, llm) = llm_setup(handle, "c3-model");
            wait(handle.create_claimed_fixture(
                NewJob {
                    configuration: llm_only(llm),
                    ..new_job(handle, "history fixture", handle.genesis(), Vec::new())
                },
                test_executor(handle),
            ))
            .expect("claim fixture")
        }

        /// A running Job that admitted a call spawning a process, so an unknown
        /// outcome leaves its stop unconfirmed.
        fn spawning_job(handle: &DatabaseHandle) -> JobId {
            let job = running_job(handle);
            wait(handle.record_metrics(NewMetrics {
                job,
                request: number(1),
                observed_at: Timestamp::now(),
                measurements: Measurements::default(),
                token: Some(TokenAnnotation::eligible(UsageFinality::Cumulative)),
            }))
            .expect("record the delivery");
            let mut call = admitted(tool_call(job, 1, 0), "c", ToolEffect::NonIdempotent);
            call.spawns = true;
            wait(handle.record_tool_decision(call)).expect("admit");
            job
        }

        fn record(handle: &DatabaseHandle, job: JobId) -> JobRecord {
            wait(handle.job(job)).expect("read").expect("a Job")
        }

        fn seqs(outputs: &[Value]) -> Vec<i64> {
            outputs
                .iter()
                .map(|output| output["seq"].as_i64().expect("a seq"))
                .collect()
        }

        #[test]
        fn a_jobs_history_is_replayed_as_stored_then_its_settlement() {
            let (_root, directory) = data_directory("c3-replay");
            let running = Running::start(&directory, config());
            let handle = running.handle.clone();
            let address = running.address();
            let job = running_job(&handle);
            for index in 0..5 {
                progress(
                    &handle,
                    job,
                    &format!("part {index}\n\"quoted\" ünïcødé\n\ndata: x"),
                );
            }
            wait(handle.finalize_job(job, JobFinalization::Completed(result("final\nanswer"))))
                .expect("complete");
            let expected = stored(address, job);
            assert_eq!(expected.len(), 6);

            let mut stream = events(address, job, None);
            assert_eq!(
                stream.headers.get("content-type").map(String::as_str),
                Some("text/event-stream; charset=utf-8")
            );
            assert_eq!(
                stream.headers.get("cache-control").map(String::as_str),
                Some("no-store")
            );
            assert_eq!(
                stream
                    .headers
                    .get("x-content-type-options")
                    .map(String::as_str),
                Some("nosniff")
            );
            assert!(!stream.headers.contains_key("content-length"));
            assert!(!stream.headers.contains_key("content-encoding"));
            assert!(!stream.headers.contains_key("set-cookie"));
            assert!(
                !stream
                    .headers
                    .keys()
                    .any(|name| name.starts_with("access-control-"))
            );
            // A snapshot of the record, then each stored output exactly as
            // the outputs route serves it, numbered by its seq, then how
            // the Job settled, and the end.
            let snapshot = stream.event().job();
            assert_eq!(
                snapshot,
                get(address, &format!("/api/v1/jobs/{job}"))["job"]
            );
            for output in &expected {
                assert_eq!(&stream.event().output(job), output);
            }
            let settled = record(&handle, job);
            let settlement = stream.event().settlement(job);
            assert_eq!(
                settlement,
                json!({
                    "api_version": 1,
                    "job": job.to_string(),
                    "settlement": {
                        "state": "completed",
                        "settled_at_us": settled.completed_at.expect("completed").as_unix_micros(),
                    },
                    "stop_confirmed_at_us": null,
                })
            );
            assert_eq!(stream.next(), None, "nothing follows the settlement");
            assert_eq!(seqs(&expected), (1..=6).collect::<Vec<_>>());
            assert_eq!(expected[5]["kind"], "result");
            assert!(!String::from_utf8_lossy(&stream.raw).contains(TOKEN));
            until(|| running.open_streams() == 0);

            // A cursor past every output fabricates nothing, and changes
            // nothing: the record, then the settlement.
            let before = record(&handle, job);
            let mut beyond = events(address, job, Some(1_000));
            assert_eq!(beyond.event().name(), "job");
            assert_eq!(beyond.event().name(), "settlement");
            assert_eq!(beyond.next(), None);
            assert_eq!(record(&handle, job), before);
            assert_eq!(stored(address, job), expected);
            running.stop();
        }

        #[test]
        fn outputs_stored_while_a_client_listens_reach_it_on_the_same_stream() {
            let (_root, directory) = data_directory("c3-live");
            let running = Running::start(&directory, config());
            let handle = running.handle.clone();
            let address = running.address();

            // Outputs stored by hand, one by one, while the stream is open.
            let job = running_job(&handle);
            progress(&handle, job, "before");
            let mut stream = events(address, job, None);
            assert_eq!(stream.event().job()["settlement"]["state"], "unsettled");
            assert_eq!(stream.event().output(job)["summary"], "before");
            for text in ["first live", "second live"] {
                let seq = progress(&handle, job, text);
                let output = stream.event().output(job);
                assert_eq!(
                    (output["seq"].as_i64(), &output["summary"]),
                    (Some(seq), &json!(text))
                );
            }
            wait(handle.finalize_job(job, JobFinalization::Failed(result("gave up"))))
                .expect("fail");
            let output = stream.event().output(job);
            assert_eq!(
                (&output["kind"], &output["summary"]),
                (&json!("result"), &json!("gave up"))
            );
            assert_eq!(
                stream.event().settlement(job)["settlement"]["state"],
                "failed"
            );
            assert_eq!(stream.next(), None);
            assert_eq!(seqs(&stored(address, job)), vec![1, 2, 3, 4]);

            // A Job admitted over HTTP and run by the execution runtime: its
            // Provider answers only once the stream is open.
            let (open, gate) = flume::bounded(1);
            let target = submissions_target(
                &handle,
                "c3-live",
                vec![Response {
                    gate: Some(gate),
                    steps: answering("streamed answer", (4, 2)),
                }],
            );
            let job = admit(address, "c3-live-key-000000001", &target);
            let mut stream = events(address, job, None);
            assert_eq!(stream.event().job()["settlement"]["state"], "unsettled");
            open.send(()).expect("let the Provider answer");
            let mut delivered = Vec::new();
            let settlement = loop {
                let event = stream.event();
                if event.name() == "settlement" {
                    break event.settlement(job);
                }
                if event.name() == "output" {
                    delivered.push(event.output(job));
                }
            };
            assert_eq!(settlement["settlement"]["state"], "completed");
            assert_eq!(
                delivered,
                stored(address, job),
                "every output, live, in order"
            );
            assert_eq!(delivered.last().expect("a result")["kind"], "result");
            assert_eq!(
                delivered.last().expect("a result")["summary"],
                "streamed answer"
            );
            assert_eq!(stream.next(), None);
            running.stop();
        }

        /// An LLM Profile on a Provider scripted by the test, and an
        /// Executor that runs it.
        fn submissions_target(
            handle: &DatabaseHandle,
            name: &str,
            responses: Vec<Response>,
        ) -> (ProfileId, ExecutorId) {
            let provider = provider(handle, name);
            let llm = llm_profile(handle, provider, llm_config(name, &[ModelCapability::Chat]));
            let executor = executor(handle, "llm worker", &[OPENAI]).expect("register");
            let mut responses = responses.into_iter();
            provider_seam::install(&format!("https://{name}.invalid/v1"), move |_| {
                responses
                    .next()
                    .expect("a scripted response for every request")
            });
            (llm, executor)
        }

        /// Submits a Job to `target` under `key` and returns it.
        fn admit(
            address: SocketAddr,
            key: &str,
            (llm, executor): &(ProfileId, ExecutorId),
        ) -> JobId {
            let body = json!({
                "llm_profile": llm.to_string(),
                "executor": executor.to_string(),
                "objective": "Say something",
                "name": key,
            })
            .to_string();
            let (host, bearer) = authorized(address);
            let length = body.len().to_string();
            let mut connection = TcpStream::connect(address).expect("connect");
            connection
                .set_read_timeout(Some(crate::test_support::WAIT))
                .expect("timeout");
            write!(
                connection,
                "POST /api/v1/jobs HTTP/1.1\r\nHost: {host}\r\nAuthorization: {bearer}\r\n\
                 Idempotency-Key: {key}\r\nContent-Type: application/json\r\n\
                 Content-Length: {length}\r\nConnection: close\r\n\r\n{body}"
            )
            .expect("send");
            let mut raw = String::new();
            connection.read_to_string(&mut raw).expect("read");
            let (head, body) = raw.split_once("\r\n\r\n").expect("a head");
            assert!(head.starts_with("HTTP/1.1 201"), "{raw}");
            let answer: Value = serde_json::from_str(body).expect("JSON");
            let job = answer["admission"]["job"].as_str().expect("a Job");
            JobId::new(job["job:".len()..].parse().expect("a number"))
        }

        #[test]
        fn a_client_resumes_after_the_last_output_it_processed() {
            let (_root, directory) = data_directory("c3-resume");
            let running = Running::start(&directory, config());
            let handle = running.handle.clone();
            let address = running.address();
            let job = running_job(&handle);
            for text in ["one", "two", "three"] {
                progress(&handle, job, text);
            }
            let mut stream = events(address, job, None);
            stream.event().job();
            let processed = (0..3)
                .map(|_| stream.event().output(job)["seq"].as_i64().expect("a seq"))
                .collect::<Vec<_>>();
            assert_eq!(processed, vec![1, 2, 3]);
            drop(stream);
            until(|| running.open_streams() == 0);
            // Disconnecting changed nothing about the Job.
            let unchanged = record(&handle, job);
            assert!(unchanged.started_at.is_some() && unchanged.unknown_at.is_none());

            for text in ["four", "five"] {
                progress(&handle, job, text);
            }
            let mut resumed = events(address, job, Some(3));
            resumed.event().job();
            assert_eq!(resumed.event().output(job)["summary"], "four");
            assert_eq!(resumed.event().output(job)["summary"], "five");
            drop(resumed);

            progress(&handle, job, "six");
            let mut queried = events_after(address, job, 5);
            queried.event().job();
            assert_eq!(queried.event().output(job)["summary"], "six");
            wait(handle.finalize_job(job, JobFinalization::Cancelled(result("cancelled"))))
                .expect("cancel");
            let output = queried.event().output(job);
            assert_eq!(
                (output["seq"].as_i64(), &output["kind"]),
                (Some(7), &json!("result"))
            );
            assert_eq!(
                queried.event().settlement(job)["settlement"]["state"],
                "cancelled"
            );
            assert_eq!(queried.next(), None);
            // A settled Job is replayed from the record alone, from any point.
            let mut tail = events_after(address, job, 6);
            tail.event().job();
            assert_eq!(tail.event().output(job)["seq"], 7);
            assert_eq!(tail.event().name(), "settlement");

            // A client that leaves while the Provider has not answered
            // leaves the Job running to its end.
            let (open, gate) = flume::bounded(1);
            let target = submissions_target(
                &handle,
                "c3-resume",
                vec![Response {
                    gate: Some(gate),
                    steps: answering("after you left", (4, 2)),
                }],
            );
            let admitted = admit(address, "c3-resume-key-0000001", &target);
            let mut left = events(address, admitted, None);
            left.event().job();
            drop(left);
            until(|| running.open_streams() == 0);
            open.send(()).expect("let the Provider answer");
            until(|| record(&handle, admitted).completed_at.is_some());
            let all = stored(address, admitted);
            let mut back = events(address, admitted, Some(0));
            back.event().job();
            let replayed = std::iter::from_fn(|| {
                let event = back.event();
                (event.name() == "output").then(|| event.output(admitted))
            })
            .collect::<Vec<_>>();
            assert_eq!(replayed, all);
            assert_eq!(all.last().expect("a result")["summary"], "after you left");
            running.stop();
        }

        #[test]
        fn pages_are_crossed_in_order_and_a_resume_at_a_page_boundary_skips_nothing() {
            let (_root, directory) = data_directory("c3-pages");
            let running = Running::start(&directory, config());
            let handle = running.handle.clone();
            let address = running.address();
            let job = running_job(&handle);
            for index in 1..=40 {
                progress(&handle, job, &format!("output {index}"));
            }
            wait(handle.finalize_job(job, JobFinalization::Completed(result("done"))))
                .expect("complete");
            // A stream reads 16 outputs at a time.
            for (after, first) in [(None, 1), (Some(16), 17), (Some(32), 33), (Some(40), 41)] {
                let mut stream = events(address, job, after);
                stream.event().job();
                let rest = stream.rest();
                let (settlement, outputs) = rest.split_last().expect("events");
                settlement.settlement(job);
                let outputs = outputs
                    .iter()
                    .map(|event| event.output(job)["seq"].as_i64().expect("a seq"))
                    .collect::<Vec<_>>();
                assert_eq!(outputs, (first..=41).collect::<Vec<_>>(), "after {after:?}");
            }
            running.stop();
        }

        #[test]
        fn a_settlement_committed_between_reads_follows_every_output() {
            let (_root, directory) = data_directory("c3-race");
            let running = Running::start(&directory, config());
            let handle = running.handle.clone();
            let address = running.address();
            // The stream has read every output and is about to look at the
            // Job again, or has read the Job and is about to read its
            // outputs, when the Job's final result is committed.
            for point in [Point::OutputsRead, Point::RecordRead] {
                let job = running_job(&handle);
                progress(&handle, job, "early");
                let (reached, release) = events_seam::hold(address.port(), job, point);
                let mut stream = events(address, job, None);
                stream.event().job();
                assert_eq!(stream.event().output(job)["summary"], "early");
                reached
                    .recv_timeout(crate::test_support::WAIT)
                    .expect("the stream holds between its reads");
                wait(handle.finalize_job(job, JobFinalization::Completed(result("the end"))))
                    .expect("complete");
                release.send(()).expect("release");
                let output = stream.event().output(job);
                assert_eq!(
                    (&output["kind"], &output["summary"]),
                    (&json!("result"), &json!("the end")),
                    "{point:?}"
                );
                assert_eq!(
                    stream.event().settlement(job)["settlement"]["state"],
                    "completed"
                );
                assert_eq!(stream.next(), None);
            }
            running.stop();
        }

        #[test]
        fn a_settlement_is_announced_only_as_the_record_concludes_it() {
            let (_root, directory) = data_directory("c3-settlements");
            let running = Running::start(&directory, config());
            let handle = running.handle.clone();
            let address = running.address();

            // Unknown, with nothing that spawns a process admitted, so its
            // local execution is confirmed stopped at once: concluded, with no
            // result it never had.
            let confirmed = running_job(&handle);
            progress(&handle, confirmed, "partial");
            wait(handle.finalize_job(confirmed, JobFinalization::Unknown)).expect("unknown");
            let mut stream = events(address, confirmed, None);
            stream.event().job();
            assert_eq!(stream.event().output(confirmed)["kind"], "progress");
            let settlement = stream.event().settlement(confirmed);
            let settled = record(&handle, confirmed);
            assert_eq!(settlement["settlement"]["state"], "unknown");
            assert_eq!(
                settlement["stop_confirmed_at_us"],
                settled
                    .stop_confirmed_at
                    .expect("confirmed")
                    .as_unix_micros()
            );
            assert_eq!(stream.next(), None);

            // Unknown, its stop not confirmed: its outputs are all there,
            // but the stream does not say it ended; once the stop is
            // confirmed, it does.
            let unconfirmed = spawning_job(&handle);
            progress(&handle, unconfirmed, "partial");
            let mut stream = events(address, unconfirmed, None);
            assert_eq!(stream.event().job()["settlement"]["state"], "unsettled");
            stream.event().output(unconfirmed);
            wait(handle.finalize_job(unconfirmed, JobFinalization::Unknown)).expect("unknown");
            let observed = stream.event().job();
            assert_eq!(observed["settlement"]["state"], "unknown");
            assert_eq!(observed["stop_confirmed_at_us"], Value::Null);
            // Heartbeats only, while the stop is not confirmed.
            let mut heartbeats = 0;
            while heartbeats < 3 {
                let event = stream.next_any().expect("the stream stays open");
                assert_eq!(event.comment.as_deref(), Some("heartbeat"), "{event:?}");
                heartbeats += 1;
            }
            wait(handle.confirm_job_stopped(unconfirmed)).expect("confirm");
            let settlement = stream.event().settlement(unconfirmed);
            assert_eq!(settlement["settlement"]["state"], "unknown");
            assert!(settlement["stop_confirmed_at_us"].is_i64());
            assert_eq!(stream.next(), None);
            running.stop();

            // A stream lasts its lifetime at most, and one that never saw a
            // conclusion ends without a settlement: an unsettled Job, and an
            // unknown one whose stop is never confirmed.
            let running = Running::start(
                &directory,
                limited(Duration::from_secs(2), Duration::from_millis(400)),
            );
            let handle = running.handle.clone();
            let address = running.address();
            let unsettled = running_job(&handle);
            let unknown = spawning_job(&handle);
            wait(handle.finalize_job(unknown, JobFinalization::Unknown)).expect("unknown");
            for (job, state) in [(unsettled, "unsettled"), (unknown, "unknown")] {
                let started = Instant::now();
                let mut stream = events(address, job, None);
                assert_eq!(stream.event().job()["settlement"]["state"], state);
                assert_eq!(stream.rest(), Vec::new(), "no settlement for {state}");
                assert!(started.elapsed() >= Duration::from_millis(400));
                assert_eq!(record(&handle, job).stop_confirmed_at, None);
            }
            until(|| running.open_streams() == 0);
            running.stop();
        }

        #[test]
        fn slow_and_vanished_clients_hold_nothing_and_slow_no_one() {
            let (_root, directory) = data_directory("c3-slow");
            let running = Running::start(
                &directory,
                limited(Duration::from_millis(500), Duration::from_secs(60)),
            );
            let handle = running.handle.clone();
            let address = running.address();
            // A long history: 600 outputs of 16 KiB, far more than the
            // connection and the sockets hold.
            let job = running_job(&handle);
            let text = "x".repeat(16 * 1024);
            for _ in 0..600 {
                progress(&handle, job, &text);
            }
            let other = running_job(&handle);

            // A client that takes nothing: the stream reads only what the
            // connection asks for, and the client is disconnected once it
            // took nothing for the stall limit. Others are answered all the
            // while.
            let (fetched_before, started) = (running.control.streams().1, Instant::now());
            let stalled = events(address, job, None);
            until(|| running.open_streams() == 0);
            assert!(started.elapsed() < Duration::from_secs(10));
            let fetched = running.control.streams().1 - fetched_before;
            assert!(
                fetched < 600,
                "{fetched} outputs were read for a client that took none"
            );
            assert_eq!(get(address, "/api/v1/health")["host"], "running");
            drop(stalled);

            // Clients that come and go leave nothing open, and the Jobs as
            // they were.
            let before = (record(&handle, job), record(&handle, other));
            for round in 0..20 {
                let mut stream = events(address, if round % 2 == 0 { job } else { other }, None);
                stream.event().job();
                drop(stream);
                until(|| running.open_streams() == 0);
            }
            assert_eq!((record(&handle, job), record(&handle, other)), before);
            // A client that reads everything, slowly, is served it all.
            let mut slow = events(address, job, Some(590));
            slow.event().job();
            for seq in 591..=600 {
                std::thread::sleep(Duration::from_millis(20));
                assert_eq!(slow.event().output(job)["seq"], seq);
            }
            drop(slow);
            running.stop();
        }

        #[test]
        fn an_output_too_large_for_an_event_is_refused_whole() {
            let (_root, directory) = data_directory("c3-large");
            let running = Running::start(&directory, config().with_response_bytes(1_024));
            let handle = running.handle.clone();
            let address = running.address();
            let job = running_job(&handle);
            let small = progress(&handle, job, "small");
            let large = progress(&handle, job, &"L".repeat(4_000));
            progress(&handle, job, "after");
            // Found while streaming: an error, never the output or a part
            // of it, and nothing after it.
            let mut stream = events(address, job, None);
            stream.event().job();
            assert_eq!(stream.event().output(job)["summary"], "small");
            let error = stream.event();
            assert_eq!((error.name(), &error.id), ("stream_error", &None));
            assert_eq!(error.json()["error"]["code"], "output_too_large");
            assert_eq!(stream.next(), None);
            assert!(!String::from_utf8_lossy(&stream.raw).contains("LLLL"));
            assert!(!String::from_utf8_lossy(&stream.raw).contains("after"));
            // Found before streaming: refused as the outputs route refuses
            // it, before anything is sent.
            let (host, bearer) = authorized(address);
            let refused = stream_with(
                address,
                &format!("GET /api/v1/jobs/{job}/events"),
                &[
                    ("Host", &host),
                    ("Authorization", &bearer),
                    ("Last-Event-ID", &small.to_string()),
                ],
            );
            assert_eq!(refused.error(), (422, "output_too_large".to_owned()));
            let paged = stream_with(
                address,
                &format!("GET /api/v1/jobs/{job}/outputs?after_seq={small}"),
                &[("Host", &host), ("Authorization", &bearer)],
            );
            assert_eq!(paged.error(), (422, "output_too_large".to_owned()));
            // A client that knowingly moves past it reads on.
            let mut past = events(address, job, Some(large));
            past.event().job();
            assert_eq!(past.event().output(job)["summary"], "after");
            drop(past);
            until(|| running.open_streams() == 0);
            running.stop();
        }

        #[test]
        fn event_streams_are_refused_as_every_route_is() {
            let (_root, directory) = data_directory("c3-security");
            let running = Running::start(&directory, config());
            let handle = running.handle.clone();
            let address = running.address();
            let job = running_job(&handle);
            let path = format!("/api/v1/jobs/{job}/events");
            let line = format!("GET {path}");
            let (host, bearer) = authorized(address);
            let refused =
                |line: &str, headers: &[(&str, &str)]| stream_with(address, line, headers).error();
            let wrong = format!("Bearer {}", TOKEN.replace('C', "D"));
            let other_origin = "http://127.0.0.1:1".to_owned();
            type Case<'a> = (&'a str, Vec<(&'a str, &'a str)>, (u16, &'a str));
            let cases: Vec<Case> = vec![
                ("no token", vec![("Host", &host)], (401, "unauthenticated")),
                (
                    "wrong token",
                    vec![("Host", &host), ("Authorization", &wrong)],
                    (401, "unauthenticated"),
                ),
                (
                    "foreign host",
                    vec![("Host", "example.com"), ("Authorization", &bearer)],
                    (403, "forbidden_host"),
                ),
                (
                    "other origin",
                    vec![
                        ("Host", &host),
                        ("Authorization", &bearer),
                        ("Origin", &other_origin),
                    ],
                    (403, "forbidden_origin"),
                ),
                (
                    "cross site",
                    vec![
                        ("Host", &host),
                        ("Authorization", &bearer),
                        ("Sec-Fetch-Site", "cross-site"),
                    ],
                    (403, "forbidden_origin"),
                ),
                (
                    "negative cursor",
                    vec![
                        ("Host", &host),
                        ("Authorization", &bearer),
                        ("Last-Event-ID", "-1"),
                    ],
                    (400, "invalid_cursor"),
                ),
                (
                    "padded cursor",
                    vec![
                        ("Host", &host),
                        ("Authorization", &bearer),
                        ("Last-Event-ID", "03"),
                    ],
                    (400, "invalid_cursor"),
                ),
                (
                    "overflowing cursor",
                    vec![
                        ("Host", &host),
                        ("Authorization", &bearer),
                        ("Last-Event-ID", "92233720368547758070"),
                    ],
                    (400, "invalid_cursor"),
                ),
                (
                    "two cursors",
                    vec![
                        ("Host", &host),
                        ("Authorization", &bearer),
                        ("Last-Event-ID", "1"),
                        ("Last-Event-ID", "2"),
                    ],
                    (400, "invalid_cursor"),
                ),
            ];
            for (case, headers, (status, code)) in cases {
                assert_eq!(
                    refused(&line, &headers),
                    (status, code.to_owned()),
                    "{case}"
                );
            }
            let with_cursor = [("Host", host.as_str()), ("Authorization", bearer.as_str())];
            for (query, code) in [
                ("?after_seq=1&after_seq=2", "invalid_cursor"),
                ("?after_seq=x", "invalid_cursor"),
                ("?after_seq=1&limit=2", "invalid_cursor"),
                ("?access_token=abc", "invalid_request"),
            ] {
                assert_eq!(
                    refused(&format!("{line}{query}"), &with_cursor),
                    (400, code.to_owned()),
                    "{query}"
                );
            }
            assert_eq!(
                refused(
                    &format!("{line}?after_seq=1"),
                    &[
                        ("Host", &host),
                        ("Authorization", &bearer),
                        ("Last-Event-ID", "1"),
                    ],
                ),
                (400, "ambiguous_cursor".to_owned())
            );
            assert_eq!(
                refused("GET /api/v1/jobs/job:999999/events", &with_cursor),
                (404, "job_not_found".to_owned())
            );
            assert_eq!(
                refused("GET /api/v1/jobs/job:01/events", &with_cursor),
                (400, "invalid_request".to_owned())
            );
            let posted = stream_with(address, &format!("POST {path}"), &with_cursor);
            assert_eq!(posted.headers.get("allow").map(String::as_str), Some("GET"));
            assert_eq!(posted.error(), (405, "method_not_allowed".to_owned()));
            // A same-origin client is served.
            let origin = format!("http://{host}");
            let mut same = stream_with(
                address,
                &line,
                &[
                    ("Host", &host),
                    ("Authorization", &bearer),
                    ("Origin", &origin),
                ],
            );
            assert_eq!(same.status, 200);
            same.event().job();
            drop(same);
            until(|| running.open_streams() == 0);

            // At most four streams at once; a fifth is refused before
            // anything is read, and served once one has closed.
            let open = (0..4)
                .map(|_| {
                    let mut stream = events(address, job, None);
                    stream.event().job();
                    stream
                })
                .collect::<Vec<_>>();
            assert_eq!(running.open_streams(), 4);
            assert_eq!(
                refused(&line, &with_cursor),
                (503, "too_many_streams".to_owned())
            );
            assert_eq!(get(address, "/api/v1/health")["host"], "running");
            drop(open);
            until(|| running.open_streams() == 0);
            events(address, job, None).event().job();
            running.stop();
        }

        #[test]
        fn stopping_the_listener_ends_its_streams_and_frees_their_resources() {
            let (_root, directory) = data_directory("c3-stop");
            let running = Running::start(&directory, config());
            let handle = running.handle.clone();
            let address = running.address();
            let job = running_job(&handle);
            progress(&handle, job, "before the stop");
            let holders = running.control.holders();
            let mut reading = events(address, job, None);
            reading.event().job();
            reading.event().output(job);
            // Another client asks and then reads nothing.
            let mut silent = events(address, job, None);
            silent.chunk();
            assert_eq!(running.open_streams(), 2);

            let Running {
                worker,
                runtime,
                handle,
                control,
            } = running;
            let stopping = Instant::now();
            wait(control.stop()).expect("stop the listener");
            let stopped = stopping.elapsed();
            assert!(stopped < Duration::from_secs(8), "stopped in {stopped:?}");
            // The stream ends without a settlement, for its client to resume.
            assert_eq!(reading.rest(), Vec::new());
            assert_eq!(holders(), 0, "nothing of the listener is left");
            TcpListener::bind(address).expect("the listener's port was released");
            assert!(TcpStream::connect(address).is_err());
            // Then, in the host's order, the runtime and the database; the
            // Job was left as it was.
            let left = record(&handle, job);
            assert!(left.started_at.is_some() && left.unknown_at.is_none());
            wait(runtime.shutdown()).expect("shutdown runtime");
            worker.shutdown().expect("shutdown");
            drop(silent);
        }
    }

    // ---- Image input ----------------------------------------------------------

    mod image_input {
        use super::*;
        use crate::openai_compatible::image::tests::{PNG_BASE64, png};
        use crate::openai_compatible::{ImageInput, ImageMime, ProviderFailure};
        use crate::provider_execution::objective_conversation_with_images;

        const OBJECTIVE: &str = "Describe what the image shows.";

        /// Settles `run` from one request of its objective with a PNG, as a
        /// plan with that image sends it; `respond` answers the request.
        /// Returns the settlement and the conversation sent.
        fn settle_with_image(
            handle: &DatabaseHandle,
            run: TokenRun,
            respond: Result<Vec<Step>, ProviderFailure>,
        ) -> (
            Result<ExecutionReceipt, ExecutionRuntimeError>,
            ChatConversation,
        ) {
            let configuration = wait(handle.job_configuration(run.job))
                .expect("read")
                .expect("a frozen configuration");
            let image = ImageInput::new(ImageMime::Png, png()).expect("a PNG");
            let conversation = objective_conversation_with_images(
                &configuration.frozen,
                Some(OBJECTIVE),
                &[image],
            );
            let handle = handle.clone();
            on_ntex(async move {
                let config = ProviderExecutionConfig::default();
                let mut sent = None;
                let mut respond = Some(respond);
                let settled = run_provider_job(
                    ProviderRun {
                        database: handle,
                        executor: run.executor,
                        input: run.input,
                        job: run.job,
                    },
                    conversation,
                    None,
                    &CancellationToken::default(),
                    async |conversation: &ChatConversation, sink: &mut DatabaseOutputSink| {
                        sent = Some(conversation.clone());
                        conversation
                            .validate()
                            .map_err(|error| ProviderExecutionError::Build {
                                message: error.to_string(),
                            })?;
                        announce(Some(&mut *sink), ProviderFact::Delivered).await;
                        match respond.take().expect("one request") {
                            Err(failure) => Err(ProviderExecutionError::Rejected { failure }),
                            Ok(steps) => {
                                read_response(
                                    payload(steps, config.cancellation.clone()),
                                    200,
                                    Instant::now(),
                                    None,
                                    &config,
                                    Some(&mut *sink),
                                    conversation,
                                )
                                .await
                            }
                        }
                    },
                )
                .await;
                (settled, sent.expect("a request was sent"))
            })
        }

        /// The objective's user message, as the Provider received it.
        fn sent_user_message(conversation: &ChatConversation) -> Value {
            let [user] = conversation.messages.as_slice() else {
                panic!("one message");
            };
            serde_json::to_value(user).expect("JSON")
        }

        fn expected_user_message() -> Value {
            json!({"role": "user", "content": [
                {"type": "text", "text": OBJECTIVE},
                {"type": "image_url", "image_url": {
                    "url": format!("data:image/png;base64,{PNG_BASE64}")
                }},
            ]})
        }

        #[test]
        fn an_answer_about_an_image_completes_the_job() {
            let (_root, directory) = data_directory("image-answer");
            let worker = DatabaseWorker::start(&directory).expect("start database");
            let handle = worker.handle();
            let (_provider, llm) = llm_setup(&handle, "model-a");
            let run = local_run(&handle, llm);
            let answer = "A single transparent pixel.";
            let (settled, sent) = settle_with_image(&handle, run, Ok(answering(answer, (85, 7))));
            settled.expect("settled");
            assert_eq!(sent_user_message(&sent), expected_user_message());
            let job = job_record(&handle, run.job);
            assert!(job.completed_at.is_some() && job.failed_at.is_none());
            let output = wait(handle.latest_output(run.job))
                .expect("read")
                .expect("a result");
            assert_eq!(output.summary.as_deref(), Some(answer));
            assert!(blocking(&handle, llm).is_empty());
            let input = wait(handle.job_input(run.input))
                .expect("read")
                .expect("input");
            assert!(
                !format!("{input:?}").contains(&PNG_BASE64[..12]),
                "the image is not stored with the Job"
            );
            worker.shutdown().expect("shutdown");
        }

        /// Every file of the data directory, as bytes, once its database is
        /// closed.
        fn stored_bytes(directory: &DataDirectory) -> Vec<(std::path::PathBuf, Vec<u8>)> {
            let mut files = Vec::new();
            let mut pending = vec![directory.root().to_path_buf()];
            while let Some(path) = pending.pop() {
                for entry in std::fs::read_dir(&path).expect("list") {
                    let path = entry.expect("entry").path();
                    if path.is_dir() {
                        pending.push(path);
                    } else {
                        let bytes = std::fs::read(&path).expect("read");
                        files.push((path, bytes));
                    }
                }
            }
            files
        }

        #[test]
        fn a_refusal_that_echoes_the_images_persists_none_of_them() {
            use crate::provider_execution::rejected_failure;
            use crate::provider_execution::tests::{
                discloses, echoing_body, image_conversations, image_urls,
            };

            let (_root, directory) = data_directory("image-refusal-echo");
            let worker = DatabaseWorker::start(&directory).expect("start database");
            let handle = worker.handle();
            let (_provider, llm) = llm_setup(&handle, "model-a");
            let mut echoed = Vec::new();
            for ((case, conversation), (status, retryable)) in image_conversations()
                .into_iter()
                .zip([(400, false), (500, true), (400, false)])
            {
                let run = local_run(&handle, llm);
                let urls = image_urls(&conversation);
                let failure = rejected_failure(
                    &conversation,
                    status,
                    Some(urls.join(",")),
                    Some(echoing_body(&urls)),
                );
                wait(settle_provider_error(
                    handle.clone(),
                    run.executor,
                    run.input,
                    run.job,
                    RequestNumber::FIRST,
                    ProviderExecutionError::Rejected { failure },
                ))
                .expect("settled");

                let job = job_record(&handle, run.job);
                assert!(
                    job.failed_at.is_some() && job.completed_at.is_none(),
                    "{case}"
                );
                let output = wait(handle.latest_output(run.job))
                    .expect("read")
                    .expect("a result");
                assert_eq!(
                    output.summary.as_deref(),
                    Some(
                        format!(
                            "provider rejected the request: HTTP {status}: \
                             provider rejected image request (HTTP {status})"
                        )
                        .as_str()
                    ),
                    "{case}"
                );
                assert!(!discloses(&format!("{output:?}"), &urls), "{case}");
                let [stat] = stats(&handle, run.job).try_into().expect("one observation");
                assert_eq!(stat.observation.status_code, Some(status), "{case}");
                let failure = stat.observation.failure.expect("a failure");
                assert_eq!(failure.status_code, Some(status), "{case}");
                assert_eq!(failure.retryable, retryable, "{case}");
                assert_eq!(
                    (
                        failure.provider_code,
                        failure.response_body,
                        failure.request_id
                    ),
                    (None, None, None),
                    "{case}"
                );
                assert_eq!(
                    failure.message,
                    format!("provider rejected image request (HTTP {status})")
                );
                assert!(blocking(&handle, llm).is_empty(), "{case}");
                echoed.extend(urls);
            }

            // The Provider's text of a text-only refusal is kept.
            let run = local_run(&handle, llm);
            let body =
                r#"{"error":{"message":"The model `m` does not exist","code":"model_not_found"}}"#;
            let failure = rejected_failure(
                &ChatConversation {
                    messages: vec![ChatMessage::user("hi")],
                    ..ChatConversation::default()
                },
                404,
                Some("req_123".to_owned()),
                Some(body.to_owned()),
            );
            wait(settle_provider_error(
                handle.clone(),
                run.executor,
                run.input,
                run.job,
                RequestNumber::FIRST,
                ProviderExecutionError::Rejected { failure },
            ))
            .expect("settled");
            let output = wait(handle.latest_output(run.job))
                .expect("read")
                .expect("a result");
            assert_eq!(
                output.summary.as_deref(),
                Some("provider rejected the request: HTTP 404: The model `m` does not exist")
            );
            let [stat] = stats(&handle, run.job).try_into().expect("one observation");
            let failure = stat.observation.failure.expect("a failure");
            assert_eq!(
                (
                    failure.provider_code.as_deref(),
                    failure.response_body.as_deref(),
                    failure.request_id.as_deref(),
                    failure.retryable
                ),
                (Some("model_not_found"), Some(body), Some("req_123"), false)
            );
            worker.shutdown().expect("shutdown");

            // Nothing of any echoed image is anywhere in the data directory,
            // the database file included, while the text refusal is.
            let files = stored_bytes(&directory);
            let database = String::from_utf8_lossy(
                &files
                    .iter()
                    .find(|(path, _)| {
                        path.extension()
                            .is_some_and(|extension| extension == "redb")
                    })
                    .expect("the database file")
                    .1,
            )
            .into_owned();
            assert!(
                database.contains("model_not_found"),
                "the probe can see stored text"
            );
            for (path, bytes) in &files {
                assert!(
                    !discloses(&String::from_utf8_lossy(bytes), &echoed),
                    "{} holds image data",
                    path.display()
                );
            }
        }

        #[test]
        fn a_provider_that_refuses_the_image_fails_the_job_with_its_own_error() {
            let (_root, directory) = data_directory("image-refused");
            let worker = DatabaseWorker::start(&directory).expect("start database");
            let handle = worker.handle();
            let (_provider, llm) = llm_setup(&handle, "model-a");
            let run = local_run(&handle, llm);
            let message = "This model does not support image input.";
            let (settled, sent) = settle_with_image(
                &handle,
                run,
                Err(ProviderFailure {
                    message: message.to_owned(),
                    status_code: Some(400),
                    provider_code: Some("invalid_request_error".to_owned()),
                    response_body: None,
                    request_id: None,
                    retry_after_ms: None,
                    retryable: false,
                }),
            );
            let receipt = settled.expect("settled");
            assert_eq!(sent_user_message(&sent), expected_user_message());
            let job = job_record(&handle, run.job);
            assert!(job.failed_at.is_some() && job.completed_at.is_none());
            let output = wait(handle.latest_output(run.job))
                .expect("read")
                .expect("a result");
            assert_eq!(Some(output.id), receipt.result.map(|result| result.output));
            let summary = output.summary.expect("a summary");
            assert!(
                summary.contains("rejected") && summary.contains(message),
                "{summary}"
            );
            assert!(blocking(&handle, llm).is_empty());
            worker.shutdown().expect("shutdown");
        }
    }

    // ---- Durable attachments ---------------------------------------------------

    mod durable_attachments {
        use std::os::unix::fs::{MetadataExt, PermissionsExt, symlink};
        use std::time::Duration;

        use super::*;
        use crate::attachments::{
            AttachmentCleanupReport, AttachmentError, AttachmentStore, Reclamation,
            ReclamationError, StoreError, publish_seam, reclaim_seam,
        };
        use crate::openai_compatible::image::tests::{PNG_BASE64, jpeg, png};
        use crate::openai_compatible::image::{MAX_IMAGE_BYTES, MAX_REQUEST_IMAGES};
        use crate::openai_compatible::{ImageError, ImageMime};
        use crate::orchestration::execution_runtime::{AttachmentImageError, attachment_images};
        use crate::persistence::{
            AttachmentCleanup, AttachmentId, AttachmentOrigin, AttachmentRecord, CleanupKind,
            ContentDigest, JobInputId, MAX_ATTACHMENT_BYTES, MAX_ATTACHMENT_NAME_BYTES,
            NewAttachment, sanitize_display_name,
        };
        use crate::provider_execution::{
            CancellationToken, EnvironmentCredentialResolver, ProviderExecutionConfig,
            objective_conversation, objective_conversation_with_images, request_body,
        };

        const OBJECTIVE: &str = "What does the image show?";

        fn new_attachment(content: Vec<u8>, mime: &str, name: Option<&str>) -> NewAttachment {
            NewAttachment {
                content,
                mime: mime.to_owned(),
                display_name: name.map(str::to_owned),
            }
        }

        fn attach(
            handle: &DatabaseHandle,
            content: Vec<u8>,
            mime: &str,
            name: &str,
        ) -> AttachmentId {
            wait(handle.create_user_attachment(new_attachment(content, mime, Some(name))))
                .expect("create an Attachment")
        }

        /// A PNG other than [`png`]: only the signature is checked.
        fn other_png() -> Vec<u8> {
            let mut bytes = png();
            bytes.extend_from_slice(b"another image");
            bytes
        }

        fn data_url(mime: &str, bytes: &[u8]) -> String {
            format!(
                "data:{mime};base64,{}",
                base64::Engine::encode(&base64::engine::general_purpose::STANDARD, bytes)
            )
        }

        fn attachment(handle: &DatabaseHandle, id: AttachmentId) -> AttachmentRecord {
            wait(handle.attachment(id))
                .expect("read")
                .expect("the Attachment exists")
        }

        fn input_attachments(handle: &DatabaseHandle, input: JobInputId) -> Vec<AttachmentId> {
            wait(handle.job_input(input))
                .expect("read")
                .expect("the input exists")
                .attachments
        }

        /// The names in the attachment directory, sorted.
        fn stored_names(directory: &DataDirectory) -> Vec<String> {
            let mut names = std::fs::read_dir(directory.attachments_path())
                .expect("list")
                .map(|entry| {
                    entry
                        .expect("entry")
                        .file_name()
                        .into_string()
                        .expect("UTF-8")
                })
                .collect::<Vec<_>>();
            names.sort();
            names
        }

        fn content_path(
            directory: &DataDirectory,
            record: &AttachmentRecord,
        ) -> std::path::PathBuf {
            directory.attachments_path().join(record.sha256.to_string())
        }

        /// The operator's manual cleanup of `id`.
        fn clean_up_manually(handle: &DatabaseHandle, id: AttachmentId) -> AttachmentCleanupReport {
            wait(handle.cleanup_attachment(id)).expect("clean up")
        }

        fn reclaimed(report: &AttachmentCleanupReport) -> Option<Reclamation> {
            report.content.as_ref().ok().copied()
        }

        fn clean_up(handle: &DatabaseHandle, id: AttachmentId, cleanup: AttachmentCleanup) {
            wait(handle.with_database(move |database| {
                crate::storage::record_attachment_cleanup(database, id, cleanup)
            }))
            .expect("record the cleanup");
        }

        /// A configured Job submitted with `attachments` and started by this
        /// process, as the runtime starts one.
        fn run_with(
            handle: &DatabaseHandle,
            llm: ProfileId,
            attachments: Vec<AttachmentId>,
        ) -> TokenRun {
            let executor = executor(handle, "llm worker", &[OPENAI]).expect("register");
            let job = wait(handle.create_claimed_fixture(
                NewJob {
                    configuration: llm_only(llm),
                    input: JobInputDraft {
                        objective: Some(OBJECTIVE.to_owned()),
                        attachments,
                        ..JobInputDraft::default()
                    },
                    ..new_job(handle, "vision", handle.genesis(), Vec::new())
                },
                test_executor(handle),
            ))
            .expect("submit the Job");
            let input = JobInputId::new(job.get());
            TokenRun {
                executor,
                job,
                input,
            }
        }

        /// What the execution sends for `job`: its conversation, built from
        /// its persisted input and Attachments, as a request body.
        fn persisted_body(
            handle: &DatabaseHandle,
            job: JobId,
        ) -> Result<Value, AttachmentImageError> {
            let input = wait(handle.job_input_for_job(job))
                .expect("read")
                .expect("the input");
            let configuration = wait(handle.job_configuration(job))
                .expect("read")
                .expect("a frozen configuration");
            let images = wait(attachment_images(handle, &input.attachments))?;
            let conversation = objective_conversation_with_images(
                &configuration.frozen,
                input.objective.as_deref(),
                &images,
            );
            conversation.validate().expect("a valid request");
            let config = &configuration.frozen.llm.config;
            Ok(request_body(&config.model, &config.parameters, &conversation).expect("a body"))
        }

        /// The exact body of an image request of `model-a`, see [`llm_config`].
        fn image_body(urls: &[String]) -> Value {
            let mut content = vec![json!({"type": "text", "text": OBJECTIVE})];
            content.extend(
                urls.iter()
                    .map(|url| json!({"type": "image_url", "image_url": {"url": url}})),
            );
            json!({
                "messages": [{"role": "user", "content": content}],
                "model": "model-a",
                "stream": true,
                "stream_options": {"include_usage": true},
                "temperature": 0.2,
                "max_tokens": 256,
            })
        }

        /// Settles `run` from one answered request of its persisted
        /// conversation, as the execution sends it.
        fn answer(handle: &DatabaseHandle, run: TokenRun, text: &str) -> ExecutionReceipt {
            let input = wait(handle.job_input_for_job(run.job))
                .expect("read")
                .expect("the input");
            let configuration = wait(handle.job_configuration(run.job))
                .expect("read")
                .expect("a frozen configuration");
            let images = wait(attachment_images(handle, &input.attachments)).expect("images");
            let conversation = objective_conversation_with_images(
                &configuration.frozen,
                input.objective.as_deref(),
                &images,
            );
            let steps = answering(text, (85, 7));
            let handle = handle.clone();
            on_ntex(async move {
                let config = ProviderExecutionConfig::default();
                let mut steps = Some(steps);
                run_provider_job(
                    ProviderRun {
                        database: handle,
                        executor: run.executor,
                        input: run.input,
                        job: run.job,
                    },
                    conversation,
                    None,
                    &CancellationToken::default(),
                    async |conversation: &ChatConversation, sink: &mut DatabaseOutputSink| {
                        conversation
                            .validate()
                            .map_err(|error| ProviderExecutionError::Build {
                                message: error.to_string(),
                            })?;
                        announce(Some(&mut *sink), ProviderFact::Delivered).await;
                        read_response(
                            payload(
                                steps.take().expect("one request"),
                                config.cancellation.clone(),
                            ),
                            200,
                            Instant::now(),
                            None,
                            &config,
                            Some(&mut *sink),
                            conversation,
                        )
                        .await
                    },
                )
                .await
            })
            .expect("settled")
        }

        #[test]
        fn an_attachment_is_an_entity_of_its_own_that_outlives_restarts() {
            let (_root, directory) = data_directory("attachment-entity");
            assert_eq!(crate::storage::FORMAT_VERSION, 16);
            let worker = DatabaseWorker::start(&directory).expect("a new format 16 database");
            let handle = worker.handle();
            let first = attach(&handle, png(), "image/png", "../shots/screen.png");
            let second = attach(&handle, jpeg(), "image/jpeg", "photo.jpg");
            assert_ne!(first, second);
            let record = attachment(&handle, first);
            assert_eq!(
                (
                    record.id,
                    record.sha256,
                    record.byte_length,
                    record.mime.as_str(),
                    record.display_name.as_deref(),
                    record.origin,
                    record.cleanup,
                ),
                (
                    first,
                    ContentDigest::of(&png()),
                    png().len() as u64,
                    "image/png",
                    Some("screen.png"),
                    AttachmentOrigin::UserProvided,
                    None,
                )
            );
            assert!(record.is_available());
            let created = attachment(&handle, second).created_at;
            assert!(record.created_at <= created);
            let listed = wait(handle.attachments(None, 10)).expect("list");
            assert_eq!(
                listed.iter().map(|record| record.id).collect::<Vec<_>>(),
                [second, first],
                "newest first"
            );
            assert_eq!(
                wait(handle.attachments(Some(second), 10)).expect("page"),
                std::slice::from_ref(&record)
            );
            worker.shutdown().expect("shutdown");

            let worker = DatabaseWorker::start(&directory).expect("restart");
            let handle = worker.handle();
            assert_eq!(attachment(&handle, first), record);
            assert_eq!(wait(handle.attachments(None, 10)).expect("list"), listed);
            let content = wait(handle.attachment_content(first)).expect("content");
            assert_eq!((content.record, content.bytes), (record, png()));
            assert!(
                wait(handle.attachment(AttachmentId::new(99)))
                    .expect("read")
                    .is_none()
            );
            assert!(matches!(
                wait(handle.attachment_content(AttachmentId::new(99))),
                Err(AttachmentError::NotFound(id)) if id == AttachmentId::new(99)
            ));
            worker.shutdown().expect("shutdown");
            assert_relationships_consistent(&directory);
        }

        #[test]
        fn identical_bytes_make_two_attachments_sharing_one_content_file() {
            let (_root, directory) = data_directory("attachment-identity");
            let worker = DatabaseWorker::start(&directory).expect("start");
            let handle = worker.handle();
            let first = attach(&handle, png(), "image/png", "a.png");
            let path = content_path(&directory, &attachment(&handle, first));
            let inode = std::fs::metadata(&path).expect("metadata").ino();
            let second = attach(&handle, png(), "image/png", "b.png");
            assert_ne!(first, second, "each upload is its own Attachment");
            let (a, b) = (attachment(&handle, first), attachment(&handle, second));
            assert_eq!(a.sha256, b.sha256);
            assert_eq!(
                (a.display_name.as_deref(), b.display_name.as_deref()),
                (Some("a.png"), Some("b.png"))
            );
            assert_eq!(
                stored_names(&directory),
                [a.sha256.to_string()],
                "one content file"
            );
            assert_eq!(std::fs::metadata(&path).expect("metadata").ino(), inode);
            assert_eq!(
                std::fs::metadata(&path).expect("metadata").mode() & 0o7777,
                0o600
            );

            // Cleaning one up leaves the other available.
            clean_up(
                &handle,
                first,
                AttachmentCleanup {
                    kind: CleanupKind::Manual,
                    cleaned_at: Timestamp::from_unix_micros(1),
                },
            );
            assert!(!attachment(&handle, first).is_available());
            assert!(attachment(&handle, second).is_available());
            assert_eq!(
                wait(handle.attachment_content(second))
                    .expect("content")
                    .bytes,
                png()
            );
            worker.shutdown().expect("shutdown");
        }

        #[test]
        fn display_names_are_sanitized_to_a_file_name() {
            for (given, kept) in [
                ("shot.png", Some("shot.png")),
                ("../../etc/passwd", Some("passwd")),
                ("C:\\Users\\me\\scan.jpg", Some("scan.jpg")),
                ("  spaced name.png  ", Some("spaced name.png")),
                ("line\nbreak\u{7}.png", Some("linebreak.png")),
                ("dir/", None),
                ("   ", None),
                ("..", None),
                ("a/.", None),
                ("", None),
            ] {
                assert_eq!(sanitize_display_name(given).as_deref(), kept, "{given:?}");
            }
            let long = format!("{}.png", "图".repeat(100));
            let kept = sanitize_display_name(&long).expect("a name");
            assert!(kept.len() <= MAX_ATTACHMENT_NAME_BYTES, "{}", kept.len());
            assert!(long.starts_with(&kept), "cut on a character boundary");

            let (_root, directory) = data_directory("attachment-name");
            let worker = DatabaseWorker::start(&directory).expect("start");
            let handle = worker.handle();
            let named = attach(&handle, png(), "image/png", "/tmp/x\r\n/../shot.png");
            assert_eq!(
                attachment(&handle, named).display_name.as_deref(),
                Some("shot.png")
            );
            let unnamed =
                wait(handle.create_user_attachment(new_attachment(png(), "image/png", None)))
                    .expect("create");
            assert_eq!(attachment(&handle, unnamed).display_name, None);
            let blank = attach(&handle, png(), "image/png", " \t ");
            assert_eq!(attachment(&handle, blank).display_name, None);
            worker.shutdown().expect("shutdown");
        }

        #[test]
        fn a_new_attachment_is_checked_before_anything_is_published() {
            let (_root, directory) = data_directory("attachment-checked");
            let worker = DatabaseWorker::start(&directory).expect("start");
            let handle = worker.handle();
            let refused =
                |attachment: NewAttachment| match wait(handle.create_user_attachment(attachment)) {
                    Err(AttachmentError::Database(error)) => violation::<()>(Err(error)),
                    other => panic!("expected a refusal, got {other:?}"),
                };
            assert_eq!(
                refused(new_attachment(Vec::new(), "image/png", None)),
                Violation::InvalidValue("content", "cannot be empty")
            );
            assert_eq!(
                refused(new_attachment(
                    vec![0; MAX_ATTACHMENT_BYTES + 1],
                    "image/png",
                    None
                )),
                Violation::ValueOutOfRange("content")
            );
            for mime in [
                "",
                "png",
                "image/PNG",
                "image/png; charset=x",
                "image/",
                "/png",
                "image/png/x",
            ] {
                assert!(
                    matches!(
                        refused(new_attachment(png(), mime, None)),
                        Violation::InvalidValue("mime", _)
                    ),
                    "{mime:?}"
                );
            }
            assert!(stored_names(&directory).is_empty(), "nothing was published");
            assert!(wait(handle.attachments(None, 10)).expect("list").is_empty());
            // Any well-formed type is an Attachment; only sending checks images.
            attach(&handle, b"plain text".to_vec(), "text/plain", "notes.txt");
            worker.shutdown().expect("shutdown");
        }

        #[test]
        fn a_job_names_its_attachments_in_order() {
            let (_root, directory) = data_directory("attachment-input");
            let worker = DatabaseWorker::start(&directory).expect("start");
            let handle = worker.handle();
            let (_provider, llm) = llm_setup(&handle, "model-a");
            let a = attach(&handle, png(), "image/png", "a.png");
            let b = attach(&handle, jpeg(), "image/jpeg", "b.jpg");
            let c = attach(&handle, other_png(), "image/png", "c.png");
            let submit = |attachments: Vec<AttachmentId>| {
                wait(handle.create_job(NewJob {
                    configuration: llm_only(llm),
                    input: JobInputDraft {
                        objective: Some(OBJECTIVE.to_owned()),
                        attachments,
                        ..JobInputDraft::default()
                    },
                    ..new_job(&handle, "vision", handle.genesis(), Vec::new())
                }))
            };
            let missing = AttachmentId::new(42);
            assert_eq!(
                violation(submit(vec![a, missing])),
                Violation::NotFound(RecordRef::Attachment(missing))
            );
            clean_up(
                &handle,
                a,
                AttachmentCleanup {
                    kind: CleanupKind::Automatic,
                    cleaned_at: Timestamp::from_unix_micros(5),
                },
            );
            assert_eq!(
                violation(submit(vec![a])),
                Violation::AttachmentCleanedUp(a)
            );
            assert!(
                wait(handle.jobs(None, 10)).expect("jobs").is_empty(),
                "refusals changed nothing"
            );

            let job = submit(vec![c, b]).expect("submit");
            let input = JobInputId::new(job.get());
            assert_eq!(
                input_attachments(&handle, input),
                [c, b],
                "in the given order"
            );
            assert!(wait(handle.attachment_inputs(a)).expect("read").is_empty());
            assert_eq!(wait(handle.attachment_inputs(b)).expect("read"), [input]);
            let submitted = wait(handle.job_input_for_job(job))
                .expect("read")
                .expect("input");
            assert_eq!((submitted.id, submitted.attachments), (input, vec![c, b]));
            worker.shutdown().expect("shutdown");

            let worker = DatabaseWorker::start(&directory).expect("restart");
            assert_eq!(
                input_attachments(&worker.handle(), input),
                [c, b],
                "frozen with the Job"
            );
            worker.shutdown().expect("shutdown");
            assert_relationships_consistent(&directory);
        }

        #[test]
        fn cleanup_keeps_the_attachment_its_metadata_and_its_references() {
            let (_root, directory) = data_directory("attachment-cleanup");
            let worker = DatabaseWorker::start(&directory).expect("start");
            let handle = worker.handle();
            let (_provider, llm) = llm_setup(&handle, "model-a");
            let manual = attach(&handle, png(), "image/png", "manual.png");
            let automatic = attach(&handle, jpeg(), "image/jpeg", "automatic.jpg");
            let run = run_with(&handle, llm, vec![manual, automatic]);
            let before = (attachment(&handle, manual), attachment(&handle, automatic));
            let cleanups = [
                AttachmentCleanup {
                    kind: CleanupKind::Manual,
                    cleaned_at: Timestamp::from_unix_micros(1_760_538_720_000_000),
                },
                AttachmentCleanup {
                    kind: CleanupKind::Automatic,
                    cleaned_at: Timestamp::from_unix_micros(1_760_538_780_000_000),
                },
            ];
            clean_up(&handle, manual, cleanups[0]);
            clean_up(&handle, automatic, cleanups[1]);
            worker.shutdown().expect("shutdown");

            let worker = DatabaseWorker::start(&directory).expect("restart");
            let handle = worker.handle();
            for (id, earlier, cleanup) in [
                (manual, &before.0, cleanups[0]),
                (automatic, &before.1, cleanups[1]),
            ] {
                let record = attachment(&handle, id);
                assert_eq!(
                    record,
                    AttachmentRecord {
                        cleanup: Some(cleanup),
                        ..earlier.clone()
                    },
                    "only the cleanup changed"
                );
                assert!(!record.is_available());
                assert!(matches!(
                    wait(handle.attachment_content(id)),
                    Err(AttachmentError::CleanedUp { id: cleaned, cleanup: recorded })
                        if cleaned == id && recorded == cleanup
                ));
            }
            assert_eq!(
                input_attachments(&handle, run.input),
                [manual, automatic],
                "the history keeps its references"
            );
            assert_eq!(
                wait(handle.attachment_inputs(manual)).expect("read"),
                [run.input]
            );
            // The restart reclaimed the content the committed cleanups
            // authorized; the records, their metadata and references stay.
            let mut reclaimed_at_open = handle.reclaimed_at_open().to_vec();
            reclaimed_at_open.sort_by_key(|(digest, _)| *digest);
            let mut expected = vec![
                (before.0.sha256, Ok(Reclamation::Removed)),
                (before.1.sha256, Ok(Reclamation::Removed)),
            ];
            expected.sort_by_key(|(digest, _)| *digest);
            assert_eq!(reclaimed_at_open, expected);
            assert!(!content_path(&directory, &before.0).exists());
            worker.shutdown().expect("shutdown");
            assert_relationships_consistent(&directory);
        }

        #[test]
        fn a_manual_cleanup_ends_availability_and_keeps_the_history() {
            let (_root, directory) = data_directory("attachment-manual");
            let worker = DatabaseWorker::start(&directory).expect("start");
            let handle = worker.handle();
            let (_provider, llm) = llm_setup(&handle, "model-a");
            let cleaned = attach(&handle, png(), "image/png", "../shots/screen.png");
            let other = attach(&handle, jpeg(), "image/jpeg", "photo.jpg");
            let run = run_with(&handle, llm, vec![cleaned, other]);
            let before = attachment(&handle, cleaned);
            let path = content_path(&directory, &before);

            let report = clean_up_manually(&handle, cleaned);
            assert!(report.recorded);
            assert_eq!(reclaimed(&report), Some(Reclamation::Removed));
            let cleanup = AttachmentCleanup {
                kind: CleanupKind::Manual,
                cleaned_at: report.record.cleanup.expect("cleaned").cleaned_at,
            };
            assert_eq!(
                report.record,
                AttachmentRecord {
                    cleanup: Some(cleanup),
                    ..before.clone()
                },
                "only the cleanup changed: digest, length, type, name, origin and creation stay"
            );
            assert!(cleanup.cleaned_at >= before.created_at);
            assert_eq!(attachment(&handle, cleaned), report.record);
            assert_eq!(
                (
                    report.record.byte_length,
                    report.record.display_name.as_deref(),
                    report.record.origin
                ),
                (
                    png().len() as u64,
                    Some("screen.png"),
                    AttachmentOrigin::UserProvided
                )
            );
            assert!(matches!(
                wait(handle.attachment_content(cleaned)),
                Err(AttachmentError::CleanedUp { id, cleanup: recorded })
                    if id == cleaned && recorded == cleanup
            ));
            assert!(!path.exists(), "its content was the only copy needed");
            assert_eq!(
                stored_names(&directory),
                [attachment(&handle, other).sha256.to_string()]
            );
            assert_eq!(
                wait(handle.attachment_content(other))
                    .expect("content")
                    .bytes,
                jpeg()
            );
            // The library still lists it, and every input still names it.
            let listed = wait(handle.attachments(None, 10)).expect("list");
            assert_eq!(listed.len(), 2);
            assert!(listed.contains(&report.record));
            assert_eq!(
                wait(handle.attachment_inputs(cleaned)).expect("read"),
                [run.input]
            );
            assert_eq!(input_attachments(&handle, run.input), [cleaned, other]);

            // Again: nothing is recorded, nothing changes.
            let again = clean_up_manually(&handle, cleaned);
            assert!(!again.recorded);
            assert_eq!(again.record, report.record);
            assert_eq!(reclaimed(&again), Some(Reclamation::Absent));

            let missing = AttachmentId::new(99);
            assert!(matches!(
                wait(handle.cleanup_attachment(missing)),
                Err(AttachmentError::NotFound(id)) if id == missing
            ));
            worker.shutdown().expect("shutdown");

            let worker = DatabaseWorker::start(&directory).expect("restart");
            let handle = worker.handle();
            assert!(worker.handle().reclaimed_at_open().is_empty());
            assert_eq!(attachment(&handle, cleaned), report.record, "durable");
            let after_restart = clean_up_manually(&handle, cleaned);
            assert!(!after_restart.recorded);
            assert_eq!(after_restart.record, report.record);
            assert_eq!(input_attachments(&handle, run.input), [cleaned, other]);
            worker.shutdown().expect("shutdown");
            assert_relationships_consistent(&directory);
        }

        #[test]
        fn an_earlier_cleanup_is_kept_and_only_its_reclamation_repeated() {
            let (_root, directory) = data_directory("attachment-earlier-cleanup");
            let worker = DatabaseWorker::start(&directory).expect("start");
            let handle = worker.handle();
            let automatic = attach(&handle, png(), "image/png", "a.png");
            let earlier = AttachmentCleanup {
                kind: CleanupKind::Automatic,
                cleaned_at: Timestamp::from_unix_micros(1_760_538_720_000_000),
            };
            // Recorded without reclaiming, as by a reclamation that failed.
            clean_up(&handle, automatic, earlier);
            let path = content_path(&directory, &attachment(&handle, automatic));
            assert!(path.exists());
            let report = clean_up_manually(&handle, automatic);
            assert!(!report.recorded);
            assert_eq!(
                report.record.cleanup,
                Some(earlier),
                "not rewritten as manual"
            );
            assert_eq!(reclaimed(&report), Some(Reclamation::Removed));
            assert!(!path.exists());
            worker.shutdown().expect("shutdown");
        }

        #[test]
        fn shared_content_is_reclaimed_only_with_its_last_available_attachment() {
            let (_root, directory) = data_directory("attachment-shared");
            let worker = DatabaseWorker::start(&directory).expect("start");
            let handle = worker.handle();
            let first = attach(&handle, png(), "image/png", "a.png");
            let second = attach(&handle, png(), "image/png", "b.png");
            let path = content_path(&directory, &attachment(&handle, first));
            let inode = std::fs::metadata(&path).expect("metadata").ino();

            let report = clean_up_manually(&handle, first);
            assert!(report.recorded);
            assert_eq!(reclaimed(&report), Some(Reclamation::Retained));
            assert_eq!(
                std::fs::metadata(&path).expect("kept").ino(),
                inode,
                "the shared file is intact"
            );
            assert!(matches!(
                wait(handle.attachment_content(first)),
                Err(AttachmentError::CleanedUp { id, .. }) if id == first
            ));
            assert_eq!(
                wait(handle.attachment_content(second))
                    .expect("content")
                    .bytes,
                png()
            );
            assert_eq!(
                reclaimed(&clean_up_manually(&handle, first)),
                Some(Reclamation::Retained),
                "still needed"
            );
            worker.shutdown().expect("shutdown");

            // A restart reclaims nothing another Attachment needs.
            let worker = DatabaseWorker::start(&directory).expect("restart");
            let handle = worker.handle();
            assert!(handle.reclaimed_at_open().is_empty());
            assert!(path.exists());
            let last = clean_up_manually(&handle, second);
            assert!(last.recorded);
            assert_eq!(reclaimed(&last), Some(Reclamation::Removed));
            assert!(!path.exists());
            let (a, b) = (attachment(&handle, first), attachment(&handle, second));
            assert_eq!(a.cleanup, report.record.cleanup, "each keeps its own");
            assert_eq!(b.cleanup, last.record.cleanup);
            assert!(
                a.cleanup.expect("cleaned").cleaned_at <= b.cleanup.expect("cleaned").cleaned_at
            );
            assert_eq!(a.sha256, b.sha256);

            // The same bytes uploaded again are a new Attachment, published anew.
            let again = attach(&handle, png(), "image/png", "c.png");
            assert!(path.exists());
            assert_eq!(
                wait(handle.attachment_content(again))
                    .expect("content")
                    .bytes,
                png()
            );
            assert!(
                attachment(&handle, first).cleanup.is_some(),
                "history stays"
            );
            worker.shutdown().expect("shutdown");
            assert_relationships_consistent(&directory);
        }

        #[test]
        fn a_cleanup_that_does_not_commit_leaves_the_attachment_available() {
            let (_root, directory) = data_directory("attachment-uncommitted");
            let worker = DatabaseWorker::start(&directory).expect("start");
            let handle = worker.handle();
            let id = attach(&handle, png(), "image/png", "a.png");
            let before = attachment(&handle, id);
            assert!(handle.activate_filesystem_protection(protection_reason(&directory)));
            assert!(matches!(
                wait(handle.cleanup_attachment(id)),
                Err(AttachmentError::Database(DatabaseError::DiskProtected(_)))
            ));
            assert_eq!(attachment(&handle, id), before, "still available");
            assert_eq!(
                wait(handle.attachment_content(id)).expect("content").bytes,
                png()
            );
            worker.shutdown().expect("shutdown");

            // As after a crash before the commit: nothing to recover.
            let worker = DatabaseWorker::start(&directory).expect("restart");
            let handle = worker.handle();
            assert!(handle.reclaimed_at_open().is_empty());
            assert_eq!(attachment(&handle, id), before);
            assert_eq!(
                wait(handle.attachment_content(id)).expect("content").bytes,
                png()
            );
            worker.shutdown().expect("shutdown");
        }

        #[test]
        fn a_failed_reclamation_keeps_the_cleanup_and_is_completed_later() {
            let (_root, directory) = data_directory("attachment-reclaim-failed");
            let worker = DatabaseWorker::start(&directory).expect("start");
            let handle = worker.handle();
            let retried = attach(&handle, png(), "image/png", "a.png");
            let recovered = attach(&handle, jpeg(), "image/jpeg", "b.jpg");
            let available = attach(&handle, other_png(), "image/png", "c.png");
            let unreferenced = handle
                .attachment_store()
                .publish(b"published, never recorded")
                .expect("publish");
            let orphan = unreferenced.digest();
            unreferenced.release();
            let paths = [retried, recovered, available]
                .map(|id| content_path(&directory, &attachment(&handle, id)));

            // The removal fails, as the file system can: the cleanup stays.
            handle
                .attachment_store()
                .install_reclaim_seam(|point| point != reclaim_seam::Point::Verified);
            for id in [retried, recovered] {
                let report = clean_up_manually(&handle, id);
                assert!(report.recorded);
                assert!(matches!(
                    report.content,
                    Err(ReclamationError::Store(StoreError::Io(_)))
                ));
                assert!(matches!(
                    wait(handle.attachment_content(id)),
                    Err(AttachmentError::CleanedUp { id: cleaned, .. }) if cleaned == id
                ));
            }
            assert!(paths.iter().all(|path| path.exists()));
            handle.attachment_store().install_reclaim_seam(|_| true);
            let report = clean_up_manually(&handle, retried);
            assert!(!report.recorded);
            assert_eq!(reclaimed(&report), Some(Reclamation::Removed));
            assert!(!paths[0].exists());
            worker.shutdown().expect("shutdown");

            // As after a crash between the commit and the removal: the next
            // start removes what the committed cleanup authorized, and only
            // that.
            let worker = DatabaseWorker::start(&directory).expect("restart");
            let handle = worker.handle();
            let digest = attachment(&handle, recovered).sha256;
            assert_eq!(
                handle.reclaimed_at_open(),
                [(digest, Ok(Reclamation::Removed))]
            );
            assert!(!paths[1].exists());
            assert!(paths[2].exists(), "an available Attachment's content stays");
            assert!(
                directory
                    .attachments_path()
                    .join(orphan.to_string())
                    .exists(),
                "content no record names is not collected"
            );
            assert!(attachment(&handle, recovered).cleanup.is_some());
            assert_eq!(
                wait(handle.attachment_content(available))
                    .expect("content")
                    .bytes,
                other_png()
            );
            worker.shutdown().expect("shutdown");

            let worker = DatabaseWorker::start(&directory).expect("restart");
            assert!(worker.handle().reclaimed_at_open().is_empty());
            worker.shutdown().expect("shutdown");
        }

        #[test]
        fn a_removal_whose_directory_sync_was_lost_is_settled_at_restart() {
            let (_root, directory) = data_directory("attachment-unsynced");
            let worker = DatabaseWorker::start(&directory).expect("start");
            let handle = worker.handle();
            let reappears = attach(&handle, png(), "image/png", "a.png");
            let stays_gone = attach(&handle, jpeg(), "image/jpeg", "b.jpg");
            let changed = attach(&handle, other_png(), "image/png", "c.png");
            let records = [reappears, stays_gone, changed].map(|id| attachment(&handle, id));
            let paths = records
                .clone()
                .map(|record| content_path(&directory, &record));
            handle
                .attachment_store()
                .install_reclaim_seam(|point| point != reclaim_seam::Point::Unlinked);
            for id in [reappears, stays_gone, changed] {
                let report = clean_up_manually(&handle, id);
                assert!(report.recorded);
                assert!(report.content.is_err(), "not reported as reclaimed");
            }
            assert!(paths.iter().all(|path| !path.exists()));
            worker.shutdown().expect("shutdown");

            // A crash may lose an unsynced removal, or keep it.
            std::fs::write(&paths[0], png()).expect("the removal was lost");
            std::fs::write(&paths[2], b"\x89PNG\r\n\x1a\nnot the recorded bytes!")
                .expect("other bytes under the name");
            for path in [&paths[0], &paths[2]] {
                std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o600))
                    .expect("chmod");
            }
            let worker = DatabaseWorker::start(&directory).expect("restart");
            let handle = worker.handle();
            let mut reclaimed_at_open = handle.reclaimed_at_open().to_vec();
            reclaimed_at_open.sort_by_key(|(digest, _)| *digest);
            let mut expected = vec![
                (records[0].sha256, Ok(Reclamation::Removed)),
                (records[2].sha256, Err(StoreError::LengthMismatch)),
            ];
            expected.sort_by_key(|(digest, _)| *digest);
            assert_eq!(reclaimed_at_open, expected);
            assert!(!paths[0].exists() && !paths[1].exists());
            assert!(paths[2].exists(), "a file that is not the content is kept");
            for record in &records {
                assert!(
                    matches!(
                        wait(handle.attachment_content(record.id)),
                        Err(AttachmentError::CleanedUp { .. })
                    ),
                    "never revived"
                );
                assert!(attachment(&handle, record.id).cleanup.is_some());
            }
            worker.shutdown().expect("shutdown");
        }

        #[test]
        fn an_upload_pinned_before_a_cleanup_keeps_its_content() {
            let (_root, directory) = data_directory("attachment-race-upload-first");
            let worker = DatabaseWorker::start(&directory).expect("start");
            let handle = worker.handle();
            let cleaned = attach(&handle, png(), "image/png", "a.png");
            let path = content_path(&directory, &attachment(&handle, cleaned));
            let (pinned, held) = flume::bounded(1);
            let (release, released) = flume::bounded::<()>(1);
            let uploader = {
                let handle = handle.clone();
                std::thread::spawn(move || {
                    publish_seam::install(move |point| {
                        if point == publish_seam::Point::Pinned {
                            pinned.send(()).ok();
                            released.recv_timeout(crate::test_support::WAIT).ok();
                        }
                        true
                    });
                    let created = wait(handle.create_user_attachment(new_attachment(
                        png(),
                        "image/png",
                        Some("again.png"),
                    )));
                    publish_seam::clear();
                    created
                })
            };
            held.recv_timeout(crate::test_support::WAIT)
                .expect("the upload has pinned the content, and recorded nothing");
            let report = clean_up_manually(&handle, cleaned);
            assert!(report.recorded);
            assert_eq!(
                reclaimed(&report),
                Some(Reclamation::Retained),
                "the content is being published"
            );
            assert!(path.exists());
            release.send(()).expect("let the upload go on");
            let uploaded = uploader
                .join()
                .expect("the uploader")
                .expect("the upload succeeds");
            assert_eq!(attachment(&handle, uploaded).sha256, report.record.sha256);
            assert_eq!(
                wait(handle.attachment_content(uploaded))
                    .expect("content")
                    .bytes,
                png(),
                "a returned Attachment has its content"
            );
            // The pin is gone once the record committed; the record now
            // protects the content, and its own cleanup reclaims it.
            assert_eq!(
                reclaimed(&clean_up_manually(&handle, cleaned)),
                Some(Reclamation::Retained)
            );
            assert_eq!(
                reclaimed(&clean_up_manually(&handle, uploaded)),
                Some(Reclamation::Removed)
            );
            assert!(!path.exists());
            worker.shutdown().expect("shutdown");
        }

        #[test]
        fn an_upload_awaiting_its_commit_keeps_its_content() {
            let (_root, directory) = data_directory("attachment-race-uncommitted");
            let worker = DatabaseWorker::start(&directory).expect("start");
            let handle = worker.handle();
            let cleaned = attach(&handle, png(), "image/png", "a.png");
            let path = content_path(&directory, &attachment(&handle, cleaned));
            // Cleaned up without reclaiming, so a retry only reclaims.
            clean_up(
                &handle,
                cleaned,
                AttachmentCleanup {
                    kind: CleanupKind::Manual,
                    cleaned_at: Timestamp::from_unix_micros(1),
                },
            );
            let (held, release) = handle.group_probe().hold_next_group();
            let uploader = {
                let handle = handle.clone();
                std::thread::spawn(move || {
                    wait(handle.create_user_attachment(new_attachment(
                        png(),
                        "image/png",
                        Some("again.png"),
                    )))
                })
            };
            held.recv_timeout(crate::test_support::WAIT)
                .expect("the upload published its content; its record is not committed");
            let report = clean_up_manually(&handle, cleaned);
            assert!(!report.recorded);
            assert_eq!(reclaimed(&report), Some(Reclamation::Retained));
            assert!(path.exists());
            release.send(()).expect("let the commit happen");
            let uploaded = uploader
                .join()
                .expect("the uploader")
                .expect("the upload succeeds");
            assert_eq!(
                wait(handle.attachment_content(uploaded))
                    .expect("content")
                    .bytes,
                png()
            );
            assert_eq!(
                reclaimed(&clean_up_manually(&handle, cleaned)),
                Some(Reclamation::Retained),
                "the new record needs it"
            );
            worker.shutdown().expect("shutdown");
        }

        #[test]
        fn an_upload_during_a_reclamation_publishes_its_content_anew() {
            let (_root, directory) = data_directory("attachment-race-reclaim-first");
            let worker = DatabaseWorker::start(&directory).expect("start");
            let handle = worker.handle();
            let cleaned = attach(&handle, png(), "image/png", "a.png");
            let path = content_path(&directory, &attachment(&handle, cleaned));
            let (verified, holding) = flume::bounded(1);
            let (release, released) = flume::bounded::<()>(1);
            let mut once = true;
            handle
                .attachment_store()
                .install_reclaim_seam(move |point| {
                    if point == reclaim_seam::Point::Verified && std::mem::take(&mut once) {
                        verified.send(()).ok();
                        released.recv_timeout(crate::test_support::WAIT).ok();
                    }
                    true
                });
            let cleaner = {
                let handle = handle.clone();
                std::thread::spawn(move || wait(handle.cleanup_attachment(cleaned)))
            };
            holding
                .recv_timeout(crate::test_support::WAIT)
                .expect("the reclamation decided to remove the file and holds the lock");
            let (done, finished) = flume::bounded(1);
            let uploader = {
                let handle = handle.clone();
                std::thread::spawn(move || {
                    let created = wait(handle.create_user_attachment(new_attachment(
                        png(),
                        "image/png",
                        Some("again.png"),
                    )));
                    done.send(()).ok();
                    created
                })
            };
            assert!(
                finished.recv_timeout(Duration::from_millis(200)).is_err(),
                "the upload waits for the reclamation"
            );
            assert!(path.exists());
            release.send(()).expect("let the reclamation go on");
            let report = cleaner.join().expect("the cleaner").expect("the cleanup");
            assert_eq!(reclaimed(&report), Some(Reclamation::Removed));
            let uploaded = uploader
                .join()
                .expect("the uploader")
                .expect("the upload succeeds");
            assert!(path.exists(), "published anew after the removal");
            assert_eq!(
                wait(handle.attachment_content(uploaded))
                    .expect("content")
                    .bytes,
                png()
            );
            assert!(matches!(
                wait(handle.attachment_content(cleaned)),
                Err(AttachmentError::CleanedUp { .. })
            ));
            worker.shutdown().expect("shutdown");
            let worker = DatabaseWorker::start(&directory).expect("restart");
            assert!(
                worker.handle().reclaimed_at_open().is_empty(),
                "the new upload's content is needed"
            );
            assert!(path.exists());
            worker.shutdown().expect("shutdown");
        }

        #[test]
        fn a_read_that_loses_the_race_with_a_cleanup_is_refused_as_cleaned_up() {
            let (_root, directory) = data_directory("attachment-race-read");
            let worker = DatabaseWorker::start(&directory).expect("start");
            let handle = worker.handle();
            let id = attach(&handle, png(), "image/png", "a.png");
            let path = content_path(&directory, &attachment(&handle, id));

            // A read whose record said available, overtaken by the cleanup
            // and its reclamation before it opens the file.
            let cleaner = handle.clone();
            let report = std::rc::Rc::new(std::cell::RefCell::new(None));
            let recorded = report.clone();
            publish_seam::install(move |point| {
                if point == publish_seam::Point::Loading && recorded.borrow().is_none() {
                    *recorded.borrow_mut() = Some(clean_up_manually(&cleaner, id));
                }
                true
            });
            let read = wait(handle.attachment_content(id));
            publish_seam::clear();
            let report = report.borrow_mut().take().expect("the cleanup ran");
            assert_eq!(reclaimed(&report), Some(Reclamation::Removed));
            assert!(!path.exists());
            assert!(matches!(
                read,
                Err(AttachmentError::CleanedUp { id: cleaned, cleanup })
                    if cleaned == id && Some(cleanup) == report.record.cleanup
            ));

            // A file missing under an available record is still reported as
            // missing, not as a cleanup.
            let missing = attach(&handle, jpeg(), "image/jpeg", "b.jpg");
            std::fs::remove_file(content_path(&directory, &attachment(&handle, missing)))
                .expect("remove");
            assert!(matches!(
                wait(handle.attachment_content(missing)),
                Err(AttachmentError::Content { id, error: StoreError::Missing }) if id == missing
            ));
            worker.shutdown().expect("shutdown");
        }

        #[test]
        fn a_cleanup_leaves_settled_jobs_alone_and_fails_unsent_ones() {
            let (_root, directory) = data_directory("attachment-cleanup-jobs");
            let worker = DatabaseWorker::start(&directory).expect("start");
            let handle = worker.handle();
            let (_provider, llm) = llm_setup(&handle, "model-a");
            let id = attach(&handle, png(), "image/png", "shot.png");
            let completed = run_with(&handle, llm, vec![id]);
            answer(&handle, completed, "One transparent pixel.");
            let pending = run_with(&handle, llm, vec![id]);
            let settled = (
                job_record(&handle, completed.job),
                wait(handle.latest_output(completed.job)).expect("read"),
                wait(handle.job_input_for_job(completed.job)).expect("read"),
            );
            assert!(settled.0.completed_at.is_some());
            let expected = image_body(&[format!("data:image/png;base64,{PNG_BASE64}")]);
            assert_eq!(
                persisted_body(&handle, pending.job).expect("a body"),
                expected
            );

            let report = clean_up_manually(&handle, id);
            assert_eq!(reclaimed(&report), Some(Reclamation::Removed));
            assert_eq!(
                (
                    job_record(&handle, completed.job),
                    wait(handle.latest_output(completed.job)).expect("read"),
                    wait(handle.job_input_for_job(completed.job)).expect("read"),
                ),
                settled,
                "a completed Job is not touched"
            );
            // The submitted Job's execution reads its images after the
            // cleanup: it is refused before any request, with nothing
            // substituted or left out.
            assert!(matches!(
                persisted_body(&handle, pending.job),
                Err(AttachmentImageError::Attachment(AttachmentError::CleanedUp { id: cleaned, .. }))
                    if cleaned == id
            ));
            assert_eq!(input_attachments(&handle, pending.input), [id]);
            assert_eq!(
                wait(handle.attachment_inputs(id)).expect("read"),
                [completed.input, pending.input]
            );
            worker.shutdown().expect("shutdown");
            assert_relationships_consistent(&directory);
        }

        #[test]
        fn a_failed_publication_commits_no_attachment() {
            let (_root, directory) = data_directory("attachment-unpublished");
            let worker = DatabaseWorker::start(&directory).expect("start");
            let handle = worker.handle();
            publish_seam::install(|point| point != publish_seam::Point::Staged);
            let staged =
                wait(handle.create_user_attachment(new_attachment(png(), "image/png", None)));
            publish_seam::clear();
            assert!(matches!(
                staged,
                Err(AttachmentError::NotStored(StoreError::Io(_)))
            ));
            assert!(stored_names(&directory).is_empty());

            // Renamed but not synced: the file stays, named by no record.
            publish_seam::install(|point| point != publish_seam::Point::Published);
            let published =
                wait(handle.create_user_attachment(new_attachment(png(), "image/png", None)));
            publish_seam::clear();
            assert!(matches!(
                published,
                Err(AttachmentError::NotStored(StoreError::Io(_)))
            ));
            assert!(wait(handle.attachments(None, 10)).expect("list").is_empty());
            let digest = ContentDigest::of(&png()).to_string();
            assert_eq!(stored_names(&directory), std::slice::from_ref(&digest));

            let id = attach(&handle, png(), "image/png", "shot.png");
            assert_eq!(stored_names(&directory), [digest], "the orphan is reused");
            assert_eq!(
                wait(handle.attachment_content(id)).expect("content").bytes,
                png()
            );
            worker.shutdown().expect("shutdown");
        }

        #[test]
        fn startup_removes_interrupted_staging_files_and_keeps_published_content() {
            let (_root, directory) = data_directory("attachment-recovery");
            let worker = DatabaseWorker::start(&directory).expect("start");
            let handle = worker.handle();
            let referenced = attach(&handle, png(), "image/png", "kept.png");
            let record = attachment(&handle, referenced);
            worker.shutdown().expect("shutdown");

            // A crash after a publication, before its record committed.
            let orphan = AttachmentStore::open(directory.attachments_path())
                .expect("open the store")
                .publish(&jpeg())
                .expect("publish")
                .digest();
            // Crashes while staging: one partial, one complete.
            let path = directory.attachments_path();
            std::fs::write(path.join(".ocg-staging-1-2-3"), &png()[..4]).expect("write");
            std::fs::write(path.join(".ocg-staging-4-5-6"), other_png()).expect("write");
            let orphan_inode = std::fs::metadata(path.join(orphan.to_string()))
                .expect("metadata")
                .ino();

            let worker = DatabaseWorker::start(&directory).expect("restart");
            let handle = worker.handle();
            let mut expected = vec![record.sha256.to_string(), orphan.to_string()];
            expected.sort();
            assert_eq!(stored_names(&directory), expected);
            assert_eq!(
                wait(handle.attachments(None, 10)).expect("list"),
                std::slice::from_ref(&record),
                "the orphan has no record"
            );
            assert_eq!(
                wait(handle.attachment_content(referenced))
                    .expect("content")
                    .bytes,
                png()
            );
            let adopted = attach(&handle, jpeg(), "image/jpeg", "late.jpg");
            assert_eq!(attachment(&handle, adopted).sha256, orphan);
            assert_eq!(
                std::fs::metadata(path.join(orphan.to_string()))
                    .expect("metadata")
                    .ino(),
                orphan_inode,
                "the published file is reused as it is"
            );
            worker.shutdown().expect("shutdown");

            // A start that finds nothing to clean changes nothing.
            let worker = DatabaseWorker::start(&directory).expect("restart");
            assert_eq!(stored_names(&directory), expected);
            worker.shutdown().expect("shutdown");
        }

        #[test]
        fn an_untrusted_attachment_directory_stops_the_database_from_starting() {
            let (_root, directory) = data_directory("attachment-untrusted");
            let path = directory.attachments_path();
            let refused = || match DatabaseWorker::start(&directory) {
                Err(DatabaseError::AttachmentStore(error)) => error,
                Err(other) => panic!("expected an attachment store refusal, got {other}"),
                Ok(worker) => {
                    worker.shutdown().ok();
                    panic!("the database must not start")
                }
            };
            std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o755)).expect("chmod");
            assert_eq!(refused(), StoreError::UntrustedDirectory);
            std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o700)).expect("chmod");
            let elsewhere = directory.root().join("elsewhere");
            std::fs::rename(path, &elsewhere).expect("move");
            symlink(&elsewhere, path).expect("link");
            std::fs::write(elsewhere.join(".ocg-staging-1-2-3"), b"x").expect("write");
            assert_eq!(refused(), StoreError::UntrustedDirectory);
            assert!(
                elsewhere.join(".ocg-staging-1-2-3").exists(),
                "nothing is cleaned through a link"
            );
            std::fs::remove_file(path).expect("unlink");
            std::fs::rename(&elsewhere, path).expect("move back");
            let worker = DatabaseWorker::start(&directory).expect("start");
            worker.shutdown().expect("shutdown");
        }

        #[test]
        fn an_attachment_directory_replaced_while_running_is_refused() {
            let (_root, directory) = data_directory("attachment-replaced");
            let worker = DatabaseWorker::start(&directory).expect("start");
            let handle = worker.handle();
            let existing = attach(&handle, png(), "image/png", "a.png");
            let path = directory.attachments_path();
            std::fs::rename(path, directory.root().join("moved")).expect("move away");
            std::fs::create_dir(path).expect("replace");
            std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o700)).expect("chmod");
            assert!(matches!(
                wait(handle.create_user_attachment(new_attachment(jpeg(), "image/jpeg", None))),
                Err(AttachmentError::NotStored(StoreError::DirectoryChanged))
            ));
            assert!(matches!(
                wait(handle.attachment_content(existing)),
                Err(AttachmentError::Content { id, error: StoreError::DirectoryChanged }) if id == existing
            ));
            assert!(
                stored_names(&directory).is_empty(),
                "the replacement gets nothing"
            );
            assert_eq!(wait(handle.attachments(None, 10)).expect("list").len(), 1);
            worker.shutdown().expect("shutdown");
        }

        #[test]
        fn images_are_rebuilt_from_the_persisted_input_in_order() {
            let (_root, directory) = data_directory("attachment-images");
            let worker = DatabaseWorker::start(&directory).expect("start");
            let handle = worker.handle();
            let (_provider, llm) = llm_setup(&handle, "model-a");
            let first = attach(&handle, other_png(), "image/png", "first.png");
            let second = attach(&handle, jpeg(), "image/jpeg", "second.jpg");
            let third = attach(&handle, png(), "image/png", "third.png");
            let run = run_with(&handle, llm, vec![second, third, first]);
            assert_eq!(
                persisted_body(&handle, run.job).expect("a body"),
                image_body(&[
                    data_url("image/jpeg", &jpeg()),
                    format!("data:image/png;base64,{PNG_BASE64}"),
                    data_url("image/png", &other_png()),
                ])
            );

            // Without Attachments the request is the text one, unchanged.
            let text = run_with(&handle, llm, Vec::new());
            let configuration = wait(handle.job_configuration(text.job))
                .expect("read")
                .expect("configured");
            let config = &configuration.frozen.llm.config;
            assert_eq!(
                persisted_body(&handle, text.job).expect("a body"),
                request_body(
                    &config.model,
                    &config.parameters,
                    &objective_conversation(&configuration.frozen, Some(OBJECTIVE))
                )
                .expect("a body")
            );
            assert_eq!(
                persisted_body(&handle, text.job).expect("a body")["messages"],
                json!([{"role": "user", "content": OBJECTIVE}])
            );
            worker.shutdown().expect("shutdown");
        }

        #[test]
        fn image_limits_are_checked_from_the_records_before_content_is_read() {
            let (_root, directory) = data_directory("attachment-limits");
            let worker = DatabaseWorker::start(&directory).expect("start");
            let handle = worker.handle();
            let images =
                |attachments: &[AttachmentId]| wait(attachment_images(&handle, attachments));
            let one = attach(&handle, png(), "image/png", "a.png");
            let many = (0..=MAX_REQUEST_IMAGES)
                .map(|_| attach(&handle, png(), "image/png", "a.png"))
                .collect::<Vec<_>>();
            assert!(matches!(images(&many), Err(AttachmentImageError::TooMany)));
            assert_eq!(
                images(&many[..MAX_REQUEST_IMAGES])
                    .expect("at the limit")
                    .len(),
                MAX_REQUEST_IMAGES
            );

            for (mime, content) in [
                ("image/gif", b"GIF89a".to_vec()),
                ("text/plain", b"text".to_vec()),
            ] {
                let other = attach(&handle, content, mime, "other");
                assert!(matches!(
                    images(&[one, other]),
                    Err(AttachmentImageError::NotAnImage { attachment, error: ImageError::UnsupportedType })
                        if attachment == other
                ));
            }
            let mut largest = png();
            largest.resize(MAX_IMAGE_BYTES, 0);
            let mut large = largest.clone();
            large.push(0);
            let large = attach(&handle, large, "image/png", "large.png");
            assert!(matches!(
                images(&[large]),
                Err(AttachmentImageError::NotAnImage { attachment, error: ImageError::TooLarge })
                    if attachment == large
            ));
            let largest = (0..5)
                .map(|_| attach(&handle, largest.clone(), "image/png", "largest.png"))
                .collect::<Vec<_>>();
            assert_eq!(images(&largest[..4]).expect("16 MiB together").len(), 4);
            assert!(matches!(
                images(&largest),
                Err(AttachmentImageError::TooLarge)
            ));
            // A type its content does not match is refused once read.
            let mislabeled = attach(&handle, jpeg(), "image/png", "fake.png");
            assert!(matches!(
                images(&[mislabeled]),
                Err(AttachmentImageError::NotAnImage { attachment, error: ImageError::Mismatch("image/png") })
                    if attachment == mislabeled
            ));
            assert_eq!(ImageMime::from_mime("image/png"), Some(ImageMime::Png));
            worker.shutdown().expect("shutdown");
        }

        #[test]
        fn a_job_is_rebuilt_after_a_restart_without_its_source_and_retried_unchanged() {
            let (_root, directory) = data_directory("attachment-restart");
            let (_workspace_root, workspace_path) =
                crate::test_support::temporary_directory(SUITE, "attachment-restart-workspace");
            std::fs::write(workspace_path.join("shot.png"), png()).expect("write the source");
            let workspace = crate::orchestration::workspace_read::Workspace::authorize(
                &workspace_path,
                &directory,
            )
            .expect("authorize");
            let source = workspace
                .read_binary("shot.png", MAX_IMAGE_BYTES)
                .expect("read the source");

            let worker = DatabaseWorker::start(&directory).expect("start");
            let handle = worker.handle();
            let (_provider, llm) = llm_setup(&handle, "model-a");
            let id = attach(&handle, source.clone(), "image/png", "shot.png");
            let run = run_with(&handle, llm, vec![id]);
            let receipt = answer(&handle, run, "One transparent pixel.");
            assert_eq!(receipt.job, run.job);
            let job = job_record(&handle, run.job);
            assert!(job.completed_at.is_some() && job.failed_at.is_none());
            assert_eq!(
                wait(handle.latest_output(run.job))
                    .expect("read")
                    .expect("result")
                    .summary
                    .as_deref(),
                Some("One transparent pixel.")
            );
            worker.shutdown().expect("shutdown");

            // The source goes away; nothing of the execution needs it.
            std::fs::rename(
                workspace_path.join("shot.png"),
                workspace_path.join("renamed.png"),
            )
            .expect("rename the source");
            std::fs::remove_file(workspace_path.join("renamed.png")).expect("remove the source");

            let worker = DatabaseWorker::start(&directory).expect("restart");
            let handle = worker.handle();
            let input = wait(handle.job_input_for_job(run.job))
                .expect("read")
                .expect("input");
            assert_eq!((input.id, input.attachments.clone()), (run.input, vec![id]));
            let content = wait(handle.attachment_content(id)).expect("content");
            assert_eq!(content.bytes, source, "the exact bytes");
            assert_eq!(ContentDigest::of(&content.bytes), content.record.sha256);
            let expected = image_body(&[format!("data:image/png;base64,{PNG_BASE64}")]);
            assert_eq!(persisted_body(&handle, run.job).expect("a body"), expected);

            let retry = wait(handle.retry_job(JobRetry {
                acknowledge_repeated_effects: false,
                ..retry_of(run.job, llm, false)
            }))
            .expect("retry");
            let retried = wait(handle.job_input_for_job(retry))
                .expect("read")
                .expect("input");
            assert_eq!(
                (retried.objective, retried.attachments),
                (input.objective, input.attachments),
                "the same objective and Attachments"
            );
            assert_eq!(persisted_body(&handle, retry).expect("a body"), expected);
            assert_eq!(
                wait(handle.attachments(None, 10)).expect("list").len(),
                1,
                "no new Attachment"
            );
            assert_eq!(
                wait(handle.attachment_inputs(id)).expect("read"),
                [run.input, JobInputId::new(retry.get())]
            );
            assert_eq!(stored_names(&directory).len(), 1);
            worker.shutdown().expect("shutdown");
            assert_relationships_consistent(&directory);
        }

        #[test]
        fn an_unusable_attachment_fails_the_job_before_any_request() {
            let (_root, directory) = data_directory("attachment-unusable");
            let worker = DatabaseWorker::start(&directory).expect("start");
            let handle = worker.handle();
            let (endpoint, accepted) = lifecycle_closure::silent_provider(1);
            let provider = connection(&handle, "silent", Some(endpoint));
            let llm = llm_profile(
                &handle,
                provider,
                llm_config("model-v", &[ModelCapability::Chat]),
            );
            let executor = executor(&handle, "llm worker", &[OPENAI]).expect("register");
            let runtime = ExecutionRuntime::start(handle.clone()).expect("start runtime");
            let schedule = |attachments: Vec<AttachmentId>, timeout: Duration| {
                wait(
                    runtime
                        .scheduler()
                        .schedule_provider(ProviderExecutionPlan {
                            executor_id: executor,
                            input: JobInputDraft {
                                objective: Some(OBJECTIVE.to_owned()),
                                attachments,
                                ..JobInputDraft::default()
                            },
                            job: NewJob {
                                configuration: llm_only(llm),
                                ..new_job(&handle, "vision", handle.genesis(), Vec::new())
                            },
                            credential_resolver: Arc::new(EnvironmentCredentialResolver),
                            provider_config: ProviderExecutionConfig {
                                timeout,
                                ..ProviderExecutionConfig::default()
                            },
                            tools: None,
                        }),
                )
            };
            let usable = attach(&handle, png(), "image/png", "usable.png");
            let missing = attach(
                &handle,
                b"\x89PNG\r\n\x1a\nmissing".to_vec(),
                "image/png",
                "m.png",
            );
            let corrupt = attach(
                &handle,
                b"\x89PNG\r\n\x1a\ncorrupt".to_vec(),
                "image/png",
                "c.png",
            );
            let gif = attach(&handle, b"GIF89a".to_vec(), "image/gif", "anim.gif");
            std::fs::remove_file(content_path(&directory, &attachment(&handle, missing)))
                .expect("remove");
            std::fs::write(
                content_path(&directory, &attachment(&handle, corrupt)),
                b"\x89PNG\r\n\x1a\nCORRUPT",
            )
            .expect("corrupt");
            for (bad, reason) in [
                (missing, "the content file is missing"),
                (
                    corrupt,
                    "the content file does not have the recorded SHA-256",
                ),
                (gif, "only image/png and image/jpeg"),
            ] {
                let receipt = schedule(vec![usable, bad], Duration::from_secs(5)).expect("settled");
                let job = job_record(&handle, receipt.job);
                assert!(
                    job.failed_at.is_some() && job.completed_at.is_none(),
                    "{reason}"
                );
                assert!(blocking(&handle, llm).is_empty(), "{reason}");
                let summary = wait(handle.latest_output(receipt.job))
                    .expect("read")
                    .expect("a result")
                    .summary
                    .expect("a summary");
                assert!(
                    summary.starts_with("the input's attachments cannot be sent: ")
                        && summary.contains(&bad.to_string())
                        && summary.contains(reason),
                    "{summary}"
                );
                assert_eq!(
                    input_attachments(&handle, receipt.input),
                    [usable, bad],
                    "kept"
                );
                assert!(
                    stats(&handle, receipt.job).is_empty(),
                    "no request was observed"
                );
                assert!(accepted.try_recv().is_err(), "nothing reached the Provider");
            }

            // A cleaned-up Attachment cannot be named by a new input; one an
            // earlier input carries is refused when its images are read.
            let cleaned = attach(&handle, other_png(), "image/png", "cleaned.png");
            let earlier = JobInputId::new(
                wait(handle.create_job(NewJob {
                    input: JobInputDraft {
                        attachments: vec![cleaned],
                        ..JobInputDraft::default()
                    },
                    ..new_job(&handle, "earlier", handle.genesis(), Vec::new())
                }))
                .expect("save")
                .get(),
            );
            clean_up(
                &handle,
                cleaned,
                AttachmentCleanup {
                    kind: CleanupKind::Manual,
                    cleaned_at: Timestamp::from_unix_micros(1),
                },
            );
            let jobs = wait(handle.jobs(None, 1_000)).expect("jobs").len();
            assert!(matches!(
                schedule(vec![usable, cleaned], Duration::from_secs(5)),
                Err(ExecutionRuntimeError::Database(DatabaseError::Rejected(
                    Violation::AttachmentCleanedUp(id)
                ))) if id == cleaned
            ));
            assert_eq!(
                wait(handle.jobs(None, 1_000)).expect("jobs").len(),
                jobs,
                "nothing submitted"
            );
            assert!(matches!(
                wait(attachment_images(&handle, &input_attachments(&handle, earlier))),
                Err(AttachmentImageError::Attachment(AttachmentError::CleanedUp { id, .. })) if id == cleaned
            ));
            assert!(accepted.try_recv().is_err(), "nothing reached the Provider");

            // Usable Attachments are read back and the request goes out.
            let receipt = schedule(vec![usable], Duration::from_millis(300)).expect("settled");
            accepted
                .recv_timeout(crate::test_support::WAIT)
                .expect("the request reached the Provider");
            assert!(
                job_record(&handle, receipt.job).unknown_at.is_some(),
                "it timed out"
            );
            wait(runtime.shutdown()).expect("shutdown runtime");
            worker.shutdown().expect("shutdown");
            assert_relationships_consistent(&directory);
        }
    }

    mod context_and_limits {
        use super::*;
        use crate::openai_compatible::{ChatMessage, ContentPartWire, MessageContentWire};
        use crate::orchestration::execution_runtime::{
            OMITTED_NOTICE, assemble_conversation, estimate_request_tokens,
            run_provider_job_with_window,
        };
        use crate::persistence::{
            MAX_STORED_TOOL_ARGUMENTS_PER_JOB_BYTES, MAX_STORED_TOOL_SCHEMAS_PER_JOB_BYTES,
        };

        #[test]
        fn item_1_context_window_profile_codec_and_freeze() {
            let (_root, directory) = data_directory("item1-context-window-freeze");
            let worker = DatabaseWorker::start(&directory).expect("start database");
            let handle = worker.handle();
            let provider = connection(&handle, "cw-prov", None);
            let mut config = llm_config("model-cw", &[ModelCapability::Chat]);
            config.context_window = Some(32_768);
            let llm = llm_profile(&handle, provider, config);

            let run = local_run(&handle, llm);
            let frozen = wait(handle.job_configuration(run.job))
                .expect("read configuration")
                .expect("configured")
                .frozen;
            assert_eq!(frozen.llm.config.context_window, Some(32_768));
            assert_eq!(frozen.llm.config.model, "model-cw");

            // Reopen database to verify storage codec decode
            worker.shutdown().expect("shutdown");
            let worker = DatabaseWorker::start(&directory).expect("reopen");
            let handle = worker.handle();
            let reopened = wait(handle.job_configuration(run.job))
                .expect("read configuration")
                .expect("configured")
                .frozen;
            assert_eq!(reopened.llm.config.context_window, Some(32_768));
            worker.shutdown().expect("shutdown");
        }

        #[test]
        fn item_2_token_estimation_and_context_window_enforcement() {
            // 1. Estimation formula: ceil(UTF-8 bytes / 3) + 1600 * images + max_tokens (or 4096)
            let conv = ChatConversation {
                messages: vec![ChatMessage::user("1234567890")], // 10 bytes -> ceil(10/3) = 4 tokens
                tools: Vec::new(),
                ..Default::default()
            };
            assert_eq!(estimate_request_tokens(&conv, Some(100)), 104);
            assert_eq!(estimate_request_tokens(&conv, None), 4100);

            // With an image part (+1600 tokens)
            let image_conv = ChatConversation {
                messages: vec![ChatMessage::user_parts(vec![
                    ContentPartWire::Text {
                        text: "1234567890".to_owned(),
                    },
                    ContentPartWire::ImageUrl {
                        image_url: crate::openai_compatible::ImageUrlWire {
                            url: "data:image/png;base64,abc".to_owned(),
                        },
                    },
                ])],
                tools: Vec::new(),
                ..Default::default()
            };
            assert_eq!(estimate_request_tokens(&image_conv, Some(100)), 104 + 1600);

            // 2. In tool loop: stops with ToolLoopStop::ContextWindow when conversation exceeds window
            let (_root, directory) = data_directory("item2-context-window-tool-loop");
            let worker = DatabaseWorker::start(&directory).expect("start database");
            let handle = worker.handle();
            let (_provider, llm) = llm_setup(&handle, "model-cw-loop");
            let run = local_run(&handle, llm);
            let (registry, _calls) = echo_registry(ToolEffect::NonIdempotent, json!({"ok": true}));

            let provider_run = ProviderRun {
                database: handle.clone(),
                executor: run.executor,
                input: run.input,
                job: run.job,
            };
            let cancellation = CancellationToken::default();
            // With context_window = 4099 and conv tokens = 4100, tool loop stops before sending
            let receipt = on_ntex(async move {
                run_provider_job_with_window(
                    provider_run,
                    conv,
                    Some(&execution(&registry, &["echo"], ToolLoopLimits::default())),
                    Some(4099),
                    Some(4096),
                    &cancellation,
                    async |_conv: &ChatConversation, _sink: &mut DatabaseOutputSink| {
                        panic!("provider exchange should not be called when context window exceeded");
                    },
                )
                .await
            });
            match receipt {
                Err(ExecutionRuntimeError::ToolLoopStopped { stop, .. }) => {
                    assert_eq!(stop, ToolLoopStop::ContextWindow);
                    assert_eq!(
                        stop.to_string(),
                        "the conversation exceeds the model's context window"
                    );
                }
                other => panic!("expected ToolLoopStopped with ContextWindow, got: {other:?}"),
            }
            let job = job_record(&handle, run.job);
            assert!(job.failed_at.is_some());

            // 3. Assemble conversation with omitted notice
            let frozen = wait(handle.job_configuration(run.job))
                .expect("read")
                .expect("configured")
                .frozen;
            let assembled = assemble_conversation(
                &frozen,
                &[(Some("prior".into()), Vec::new(), "prior answer".into())],
                true,
                Some("current"),
                &[],
                None,
                Vec::new(),
            );
            let sys_text = assembled
                .messages
                .iter()
                .find_map(|m| {
                    if m.role == crate::openai_compatible::ChatRole::System {
                        match &m.content {
                            Some(MessageContentWire::Text(t)) => Some(t.as_str()),
                            _ => None,
                        }
                    } else {
                        None
                    }
                })
                .expect("system message");
            assert!(
                sys_text.contains(OMITTED_NOTICE),
                "system message should contain omitted notice: {sys_text}"
            );

            worker.shutdown().expect("shutdown");
        }

        #[test]
        fn item_3_unified_tool_loop_limits_and_running_totals() {
            // Verify unified defaults
            let limits = ToolLoopLimits::default();
            assert_eq!(limits.max_provider_requests, 64);
            assert_eq!(limits.max_tool_calls, 256);
            assert_eq!(limits.max_tool_result_bytes, 16 * 1024);
            assert_eq!(limits.max_tool_results_bytes, 1024 * 1024);
            assert_eq!(MAX_STORED_TOOL_ARGUMENTS_PER_JOB_BYTES, 4 * 1024 * 1024);
            assert_eq!(MAX_STORED_TOOL_SCHEMAS_PER_JOB_BYTES, 4 * 1024 * 1024);

            // Verify running-counter TOOL_TOTALS enforces per-Job totals across multiple calls
            let (_root, directory) = data_directory("item3-running-totals");
            let worker = DatabaseWorker::start(&directory).expect("start database");
            let handle = worker.handle();
            let (_provider, llm) = llm_setup(&handle, "model-limits");
            let run = observed_run(&handle, llm, 1);
            let call = |idx| tool_call(run.job, 1, idx);

            // Admitting a call with large arguments (within single-call limit of 64 KiB, e.g. 60 KiB)
            let args_chunk = text_arguments("", 60 * 1024);
            // 4 MiB / 60 KiB = 68.26 chunks. First 68 succeed (68 * 60 KiB = 4080 KiB <= 4096 KiB)
            for i in 0..68 {
                wait(handle.record_tool_decision(admitted_with(
                    call(i),
                    &format!("call_{i}"),
                    ToolEffect::ReadOnly,
                    &args_chunk,
                )))
                .expect("fits within 4 MiB running total");
            }

            // 69th call (69 * 60 KiB = 4140 KiB > 4096 KiB) must be rejected by running total
            let rejected = wait(handle.record_tool_decision(admitted_with(
                call(68),
                "call_68",
                ToolEffect::ReadOnly,
                &args_chunk,
            )));
            assert_eq!(
                violation(rejected),
                Violation::ToolEvidenceLimit {
                    call: call(68),
                    limit: "the arguments of one Job",
                }
            );

            // Reopening database verifies running totals consistency in storage
            worker.shutdown().expect("shutdown");
            let worker = DatabaseWorker::start(&directory).expect("restart");
            worker.shutdown().expect("shutdown");
            assert_relationships_consistent(&directory);
        }
    }
}
