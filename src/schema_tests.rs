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
    AgentConfig, AvailabilityAggregationProgress, AvailabilityBatch, AvailabilityBatchReport,
    AvailabilityClassification, AvailabilityCounts, AvailabilityObservation,
    AvailabilityRequestSample, AvailabilityStatId, AvailabilityStatRecord, DependencyCondition,
    DependencyState, ExecutionConfiguration, ExecutionMode, ExecutorId, ExecutorType, FinalResult,
    IntermediateOutputKind, JobFinalization, JobId, JobInputDraft, JobOutcome, JobRecord, JobRetry,
    LlmConfig, LlmParameters, LlmProviderId, LlmTarget, LlmTransport, Measurements, MetricsRecord,
    MixedRequests, ModelCapability, NewAgent, NewAvailabilityStat, NewExecutor, NewJob,
    NewJobOutput, NewLlmProvider, NewMetrics, NewProfile, NewProfileConfig, NewProject,
    ObservationCursor, ObservationFilter, OutputKind, ProfileId, ProfileKind, ProjectId, RecordRef,
    RejectionCause, RequestNumber, RequestRef, RetryInput, TimeRange, Timestamp, UnavailableCause,
    Violation, WrongProfileKind, minute_of,
};
use crate::provider_execution::{ProviderExecutionError, ResponseFacts};
use crate::test_support::{TestRoot, data_directory as test_data_directory, poll_once, wait};

const SUITE: &str = "ocg-schema";
/// The transport of the only LLM adapter OCG implements.
const OPENAI: LlmTransport = LlmTransport::OpenAiChatCompletions;

fn data_directory(name: &str) -> (TestRoot, DataDirectory) {
    test_data_directory(SUITE, name)
}

fn new_job(name: &str, parent: JobId, projects: Vec<ProjectId>) -> NewJob {
    NewJob {
        name: Some(name.into()),
        execution_mode: ExecutionMode::Once,
        eligible_at: None,
        scheduled_at: None,
        next_due_at: None,
        next_iteration_no: None,
        iteration_no: None,
        parent,
        projects,
        dependencies: Vec::new(),
        configuration: None,
        assigned_executor: None,
        previous: None,
        replaces: None,
    }
}

fn create_job(handle: &DatabaseHandle, job: NewJob) -> JobId {
    let input = wait(handle.save_job_input(
        None,
        JobInputDraft {
            objective: job.name.clone(),
            ..JobInputDraft::default()
        },
    ))
    .expect("save input");
    wait(handle.create_job_with_input(job, input)).expect("create job")
}

fn violation<T: std::fmt::Debug>(result: Result<T, DatabaseError>) -> Violation {
    match result {
        Err(DatabaseError::Rejected(violation)) => violation,
        other => panic!("expected a rule violation, got {other:?}"),
    }
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
    wait(handle.create_llm_provider(NewLlmProvider {
        name: name.into(),
        transport: OPENAI,
        endpoint: Some(format!("https://{name}.invalid/v1")),
        credential_ref: Some(format!("{}_KEY", name.to_uppercase())),
    }))
    .expect("create provider")
}

fn llm_config(model: &str, declared: &[ModelCapability]) -> LlmConfig {
    LlmConfig {
        model: model.into(),
        parameters: LlmParameters {
            temperature: Some(0.2),
            max_tokens: Some(256),
            ..LlmParameters::default()
        },
        declared_capabilities: declared.iter().copied().collect(),
    }
}

fn llm_profile(handle: &DatabaseHandle, provider: LlmProviderId, config: LlmConfig) -> ProfileId {
    wait(handle.create_profile(NewProfile {
        name: config.model.clone(),
        config: NewProfileConfig::Llm { provider, config },
    }))
    .expect("create LLM Profile")
}

fn agent_profile(
    handle: &DatabaseHandle,
    instructions: &str,
    llm_targets: Vec<ProfileId>,
) -> ProfileId {
    wait(handle.create_profile(NewProfile {
        name: "assistant".into(),
        config: NewProfileConfig::Agent {
            config: AgentConfig {
                instructions: Some(instructions.into()),
            },
            llm_targets,
        },
    }))
    .expect("create Agent Profile")
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

/// Reads every page of a query with `page` records each, checking that
/// every cursor moves forward.
fn every_page<T, I: Copy + Ord + std::fmt::Debug>(
    mut read: impl FnMut(Option<ObservationCursor<I>>) -> crate::persistence::ObservationPage<T, I>,
) -> Vec<T> {
    let (mut records, mut after) = (Vec::new(), None);
    for _ in 0..10_000 {
        let page = read(after);
        records.extend(page.records);
        match page.next {
            None => return records,
            Some(next) => {
                if let Some(previous) = after {
                    assert!(
                        (next.observed_at, next.record) > (previous.observed_at, previous.record),
                        "the cursor must advance"
                    );
                }
                after = Some(next);
            }
        }
    }
    panic!("the query did not finish");
}

fn all_metrics(
    handle: &DatabaseHandle,
    filter: &ObservationFilter,
    page: usize,
) -> Vec<MetricsRecord> {
    every_page(|after| wait(handle.metrics(filter.clone(), after, page)).expect("read Metrics"))
}

fn all_availability(
    handle: &DatabaseHandle,
    filter: &ObservationFilter,
    page: usize,
) -> Vec<AvailabilityStatRecord> {
    every_page(|after| {
        wait(handle.availability_stats(filter.clone(), after, page))
            .expect("read AvailabilityStats")
    })
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
    let assistant = agent_profile(&handle, "Answer briefly.", Vec::new());
    let agent = wait(handle.create_agent(NewAgent {
        name: "helper".into(),
        profiles: vec![assistant],
    }))
    .expect("create Agent");
    let executor = wait(handle.create_executor(NewExecutor {
        name: "worker".into(),
        executor_type: ExecutorType::Llm,
        concurrency_limit: NonZeroU32::MIN,
        llm_transports: BTreeSet::from([OPENAI]),
    }))
    .expect("create Executor");
    let parent = create_job(
        &handle,
        NewJob {
            assigned_executor: Some(executor),
            ..new_job("parent", genesis, vec![alpha, beta])
        },
    );
    let child = create_job(&handle, new_job("child", parent, Vec::new()));
    let dependent = create_job(
        &handle,
        NewJob {
            dependencies: vec![(parent, DependencyCondition::Settled)],
            ..new_job("dependent", genesis, vec![beta])
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
            assigned_executor: Some(executor),
            ..new_job("chat check", genesis, Vec::new())
        },
    );
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
            wait(handle.llm_providers(None, 100)).expect("read providers"),
            wait(handle.profiles(None, 100)).expect("read Profiles"),
            wait(handle.agent(agent)).expect("read Agent"),
            wait(handle.job_metrics(check, None, 100)).expect("read Metrics"),
            wait(handle.children(parent, None, 100)).expect("read children"),
            wait(handle.provider_profiles(first_provider)).expect("read provider Profiles"),
            wait(handle.job_availability(check, None, 100)).expect("read AvailabilityStats"),
            wait(handle.job_outputs(check, None, 100)).expect("read check outputs"),
        )
    };
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
        providers,
        profiles,
        agent_record,
        metrics,
        children,
        provider_profiles,
        availability,
        check_outputs,
    ) = after;
    assert_eq!(jobs.len(), 5, "Genesis and the four created Jobs");
    let parent_record = jobs
        .iter()
        .find(|job| job.id == parent)
        .expect("parent persisted");
    assert_eq!(
        (parent_record.parent, parent_record.projects.clone()),
        (genesis, vec![alpha, beta])
    );
    assert_eq!(parent_record.assigned_executor, Some(executor));
    assert_eq!(outputs.len(), 1);
    assert_eq!(projects.len(), 2);
    assert_eq!(beta_jobs, [parent, dependent]);
    assert_eq!(
        providers.len(),
        2,
        "the schema itself does not limit the number of Providers"
    );
    for kind in [ProfileKind::Agent, ProfileKind::Llm] {
        assert!(
            profiles.iter().any(|profile| profile.config.kind() == kind),
            "a {kind} Profile is in the Profile table"
        );
    }
    assert_eq!(agent_record.expect("Agent persisted").profiles, [assistant]);
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
    assert_eq!(provider_profiles, [llm]);
    assert!(
        wait(handle.provider_profiles(second_provider))
            .expect("read provider Profiles")
            .is_empty()
    );
    worker.shutdown().expect("shutdown");
}

#[test]
fn genesis_is_one_protected_root_that_is_never_its_own_child() {
    let (_root, directory) = data_directory("genesis");
    let worker = DatabaseWorker::start(&directory).expect("start database");
    let handle = worker.handle();
    let genesis = handle.genesis();
    let record = wait(handle.job(genesis))
        .expect("read Genesis")
        .expect("Genesis exists");
    assert_eq!(
        record.parent, genesis,
        "the root marker: Genesis is its own parent"
    );
    assert!(
        wait(handle.children(genesis, None, 100))
            .expect("read children")
            .is_empty()
    );

    let top = create_job(&handle, new_job("top", genesis, Vec::new()));
    assert_eq!(
        wait(handle.children(genesis, None, 100)).expect("read children"),
        [top]
    );

    for finalization in [
        JobFinalization::Cancelled(result("stop everything")),
        JobFinalization::Completed(result("done")),
        JobFinalization::Failed(result("broken")),
        JobFinalization::Unknown,
    ] {
        assert_eq!(
            violation(wait(handle.finalize_job(genesis, finalization))),
            Violation::GenesisProtected(genesis)
        );
    }
    let retry = JobRetry {
        original: genesis,
        input: RetryInput::Unchanged,
        name: None,
        execution_mode: ExecutionMode::Once,
        eligible_at: None,
        scheduled_at: None,
        next_due_at: None,
        next_iteration_no: None,
        iteration_no: None,
        projects: Vec::new(),
        dependencies: Vec::new(),
        configuration: None,
        assigned_executor: None,
        acknowledge_repeated_effects: false,
    };
    assert_eq!(
        violation(wait(handle.retry_job(retry))),
        Violation::GenesisProtected(genesis)
    );
    assert_eq!(
        wait(handle.job(genesis)).expect("read Genesis"),
        Some(record.clone()),
        "refusals left Genesis untouched"
    );
    assert!(
        wait(handle.job_outputs(genesis, None, 10))
            .expect("read outputs")
            .is_empty()
    );

    // Genesis waits for its ordinary children only, never for itself.
    wait(handle.finalize_job(top, JobFinalization::Completed(result("done"))))
        .expect("finalize a child of Genesis");
    worker.shutdown().expect("shutdown");

    let worker = DatabaseWorker::start(&directory).expect("reopen database");
    let handle = worker.handle();
    assert_eq!(
        handle.genesis(),
        genesis,
        "the persisted Genesis identity is reused"
    );
    let roots: Vec<_> = wait(handle.jobs(None, 100))
        .expect("read jobs")
        .into_iter()
        .filter(|job| job.parent == job.id)
        .map(|job| job.id)
        .collect();
    assert_eq!(roots, [genesis], "exactly one Job is its own parent");
    worker.shutdown().expect("shutdown");
}

#[test]
fn job_creation_refuses_missing_endpoints_without_partial_writes() {
    let (_root, directory) = data_directory("endpoints");
    let worker = DatabaseWorker::start(&directory).expect("start database");
    let handle = worker.handle();
    let genesis = handle.genesis();
    let existing = project(&handle, "existing");
    let input = wait(handle.save_job_input(
        None,
        JobInputDraft {
            objective: Some("try".into()),
            ..JobInputDraft::default()
        },
    ))
    .expect("save input");
    let jobs_before = wait(handle.jobs(None, 100)).expect("read jobs");

    let missing_job = JobId::new(9_999);
    let missing_project = ProjectId::new(9_999);
    let attempts = [
        (
            new_job("orphan", missing_job, Vec::new()),
            Violation::NotFound(RecordRef::Job(missing_job)),
        ),
        (
            new_job("lost", genesis, vec![existing, missing_project]),
            Violation::NotFound(RecordRef::Project(missing_project)),
        ),
        (
            NewJob {
                dependencies: vec![(missing_job, DependencyCondition::Success)],
                ..new_job("waits", genesis, Vec::new())
            },
            Violation::NotFound(RecordRef::Job(missing_job)),
        ),
        (
            new_job("twice", genesis, vec![existing, existing]),
            Violation::DuplicateReference(RecordRef::Project(existing)),
        ),
    ];
    for (job, expected) in attempts {
        assert_eq!(
            violation(wait(handle.create_job_with_input(job, input))),
            expected
        );
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
    let input_record = wait(handle.job_input(input))
        .expect("read input")
        .expect("input exists");
    assert_eq!(
        input_record.submitted_at, None,
        "the input was not frozen by a refused submission"
    );
    wait(handle.save_job_input(Some(input), JobInputDraft::default()))
        .expect("the input is still editable");
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

    let parent = create_job(&handle, new_job("parent", genesis, vec![alpha, beta]));
    let unassociated = create_job(&handle, new_job("unassociated", parent, Vec::new()));
    let elsewhere = create_job(&handle, new_job("elsewhere", parent, vec![gamma]));
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
fn both_profile_kinds_share_one_table_and_typed_reads_refuse_the_other_kind() {
    let (_root, directory) = data_directory("profiles");
    let worker = DatabaseWorker::start(&directory).expect("start database");
    let handle = worker.handle();
    let llm_provider = provider(&handle, "first");
    let llm = llm_profile(
        &handle,
        llm_provider,
        llm_config("model-a", &[ModelCapability::Chat]),
    );
    let assistant = agent_profile(&handle, "Answer briefly.", Vec::new());

    let all = wait(handle.profiles(None, 100)).expect("read Profiles");
    assert_eq!(
        all.iter()
            .map(|profile| (profile.id, profile.config.kind()))
            .collect::<Vec<_>>(),
        [(assistant, ProfileKind::Agent), (llm, ProfileKind::Llm)]
    );
    let typed_llm = wait(handle.llm_profile(llm))
        .expect("read LLM Profile")
        .expect("LLM Profile exists");
    assert_eq!(
        (typed_llm.config.model.as_str(), typed_llm.links),
        ("model-a", llm_provider)
    );
    let typed_agent = wait(handle.agent_profile(assistant))
        .expect("read Agent Profile")
        .expect("Agent Profile exists");
    assert_eq!(
        typed_agent.config.instructions.as_deref(),
        Some("Answer briefly.")
    );
    match wait(handle.agent_profile(llm)) {
        Err(DatabaseError::WrongProfileKind(mismatch)) => {
            assert_eq!(
                mismatch,
                WrongProfileKind {
                    profile: llm,
                    expected: ProfileKind::Agent,
                    found: ProfileKind::Llm
                }
            )
        }
        other => panic!("an LLM Profile is not an Agent Profile, got {other:?}"),
    }
    assert!(matches!(
        wait(handle.llm_profile(assistant)),
        Err(DatabaseError::WrongProfileKind(_))
    ));

    let wrong_kind = violation(wait(handle.create_agent(NewAgent {
        name: "confused".into(),
        profiles: vec![llm],
    })));
    assert_eq!(
        wrong_kind,
        Violation::WrongProfileKind(WrongProfileKind {
            profile: llm,
            expected: ProfileKind::Agent,
            found: ProfileKind::Llm
        })
    );
    let agent = wait(handle.create_agent(NewAgent {
        name: "helper".into(),
        profiles: vec![assistant],
    }))
    .expect("create Agent");
    assert_eq!(
        wait(handle.agent(agent))
            .expect("read Agent")
            .expect("Agent exists")
            .profiles,
        [assistant]
    );

    let missing = LlmProviderId::new(9_999);
    let refused = [
        (
            NewProfileConfig::Llm {
                provider: missing,
                config: llm_config("model-b", &[]),
            },
            Violation::NotFound(RecordRef::LlmProvider(missing)),
        ),
        (
            NewProfileConfig::Llm {
                provider: llm_provider,
                config: llm_config("", &[]),
            },
            Violation::InvalidValue("model", "cannot be empty"),
        ),
        (
            NewProfileConfig::Llm {
                provider: llm_provider,
                config: LlmConfig {
                    parameters: LlmParameters {
                        top_p: Some(f64::NAN),
                        ..LlmParameters::default()
                    },
                    ..llm_config("model-c", &[])
                },
            },
            Violation::InvalidValue("top_p", "must be a finite number"),
        ),
    ];
    for (config, expected) in refused {
        assert_eq!(
            violation(wait(handle.create_profile(NewProfile {
                name: "refused".into(),
                config
            }))),
            expected
        );
    }
    assert_eq!(
        violation(wait(handle.create_llm_provider(NewLlmProvider {
            name: "blank".into(),
            transport: OPENAI,
            endpoint: None,
            credential_ref: Some(String::new())
        }))),
        Violation::InvalidValue("credential_ref", "cannot be empty")
    );
    assert_eq!(
        wait(handle.profiles(None, 100))
            .expect("read Profiles")
            .len(),
        2,
        "refused Profiles were not stored"
    );
    worker.shutdown().expect("shutdown");
    assert_relationships_consistent(&directory);
}

#[test]
fn capability_checks_are_ordinary_llm_only_jobs_kept_apart_from_declarations() {
    let (_root, directory) = data_directory("capability-checks");
    let worker = DatabaseWorker::start(&directory).expect("start database");
    let handle = worker.handle();
    let genesis = handle.genesis();
    let llm_provider = provider(&handle, "first");
    let llm = llm_profile(
        &handle,
        llm_provider,
        llm_config("model-a", &[ModelCapability::Chat]),
    );
    let declaration = wait(handle.profile(llm))
        .expect("read Profile")
        .expect("Profile exists");
    let checker =
        executor(&handle, "checker", ExecutorType::Llm, &[OPENAI]).expect("register Executor");
    let assistant = agent_profile(&handle, "Answer briefly.", Vec::new());
    let input = wait(handle.save_job_input(
        None,
        JobInputDraft {
            objective: Some("check tool calling".into()),
            ..JobInputDraft::default()
        },
    ))
    .expect("save input");

    let missing = ProfileId::new(9_999);
    let refused = [
        (
            llm_only(assistant),
            Violation::WrongProfileKind(WrongProfileKind {
                profile: assistant,
                expected: ProfileKind::Llm,
                found: ProfileKind::Agent,
            }),
        ),
        (
            llm_only(missing),
            Violation::NotFound(RecordRef::Profile(missing)),
        ),
        // An Agent Profile, when one is named, is still validated.
        (
            configuration(llm, llm),
            Violation::WrongProfileKind(WrongProfileKind {
                profile: llm,
                expected: ProfileKind::Agent,
                found: ProfileKind::Llm,
            }),
        ),
        (
            configuration(assistant, llm),
            Violation::NotAConfiguredTarget {
                agent_profile: assistant,
                llm_profile: llm,
            },
        ),
    ];
    let jobs_before = wait(handle.jobs(None, 100)).expect("read jobs");
    for (configuration, expected) in refused {
        let job = NewJob {
            configuration,
            assigned_executor: Some(checker),
            ..new_job("tools check", genesis, Vec::new())
        };
        assert_eq!(
            violation(wait(handle.create_job_with_input(job, input))),
            expected
        );
    }
    assert_eq!(
        wait(handle.jobs(None, 100)).expect("read jobs"),
        jobs_before,
        "no partial Job was committed"
    );
    assert!(
        wait(handle.profile_jobs(llm, None, 100))
            .expect("read Profile Jobs")
            .is_empty()
    );
    assert_eq!(
        wait(handle.job_input(input))
            .expect("read input")
            .expect("input exists")
            .configuration,
        None,
        "the input was neither frozen nor given a configuration"
    );

    // The server checks a capability the user never declared, with an LLM
    // Executor and no Agent or Agent Profile at all.
    let check = wait(handle.create_job_with_input(
        NewJob {
            configuration: llm_only(llm),
            assigned_executor: Some(checker),
            ..new_job("tools check", genesis, Vec::new())
        },
        input,
    ))
    .expect("submit an LLM-only check");
    let submitted = wait(handle.job_configuration(check))
        .expect("read configuration")
        .expect("the check is configured");
    assert_eq!(
        (
            submitted.agent_profile,
            submitted.llm_profile,
            &submitted.frozen.agent
        ),
        (None, llm, &None),
        "no Agent configuration is made up for LLM-only work"
    );
    assert_eq!(
        (
            submitted.frozen.llm.config.model.as_str(),
            submitted.frozen.llm.provider.id,
            submitted.frozen.llm.provider.transport
        ),
        ("model-a", llm_provider, OPENAI)
    );
    assert_eq!(
        wait(handle.job(check))
            .expect("read job")
            .expect("job exists")
            .configuration,
        llm_only(llm)
    );
    assert_eq!(
        blocking(&handle, llm),
        [check],
        "the submitted check holds its LLM Profile"
    );

    wait(handle.start_job(check)).expect("start");
    let unsupported = json!({"capability": "tools", "observed": "unsupported", "evidence": "no tool call in the response"});
    wait(handle.finalize_job(
        check,
        JobFinalization::Completed(FinalResult {
            value: Some(unsupported.clone()),
            ..result("the check ran to the end")
        }),
    ))
    .expect("publish the conclusion");
    let record = wait(handle.job(check))
        .expect("read job")
        .expect("job exists");
    assert!(
        record.completed_at.is_some() && record.failed_at.is_none(),
        "a negative conclusion does not fail the Job"
    );
    assert_eq!(
        wait(handle.latest_output(check))
            .expect("read latest output")
            .and_then(|output| output.value),
        Some(unsupported.clone())
    );

    // A positive conclusion does not complete a Job either.
    let cancelled = create_job(
        &handle,
        NewJob {
            configuration: llm_only(llm),
            ..new_job("chat check", genesis, Vec::new())
        },
    );
    let supported = json!({"capability": "chat", "observed": "supported"});
    wait(handle.append_job_output(
        cancelled,
        NewJobOutput {
            kind: IntermediateOutputKind::Evaluation,
            summary: None,
            code: None,
            goal_reached: Some(true),
            value: Some(supported.clone()),
        },
    ))
    .expect("publish an evaluation");
    wait(handle.finalize_job(
        cancelled,
        JobFinalization::Cancelled(result("stopped by the user")),
    ))
    .expect("cancel");
    let record = wait(handle.job(cancelled))
        .expect("read job")
        .expect("job exists");
    assert!(record.cancelled_at.is_some() && record.completed_at.is_none());
    assert_eq!(
        wait(handle.profile(llm)).expect("read Profile"),
        Some(declaration.clone()),
        "neither conclusion edits the declared capabilities"
    );

    // Checking again is a new Job.
    let again = wait(handle.retry_job(JobRetry {
        original: cancelled,
        input: RetryInput::Unchanged,
        name: Some("chat check again".into()),
        execution_mode: ExecutionMode::Once,
        eligible_at: None,
        scheduled_at: None,
        next_due_at: None,
        next_iteration_no: None,
        iteration_no: None,
        projects: Vec::new(),
        dependencies: Vec::new(),
        configuration: llm_only(llm),
        assigned_executor: Some(checker),
        acknowledge_repeated_effects: false,
    }))
    .expect("retry the check");
    let again_record = wait(handle.job(again))
        .expect("read retry")
        .expect("retry exists");
    assert_eq!(
        (
            again_record.retry_of,
            again_record.input,
            again_record.configuration
        ),
        (Some(cancelled), record.input, llm_only(llm))
    );
    assert_eq!(wait(handle.job(cancelled)).expect("read job"), Some(record));
    let outputs = |handle: &DatabaseHandle| {
        [check, cancelled].map(|job| {
            wait(handle.job_outputs(job, None, 100))
                .expect("read outputs")
                .into_iter()
                .map(|output| output.value)
                .collect::<Vec<_>>()
        })
    };
    assert_eq!(
        outputs(&handle),
        [vec![Some(unsupported)], vec![Some(supported), None]]
    );
    let before = outputs(&handle);
    worker.shutdown().expect("shutdown");

    let worker = DatabaseWorker::start(&directory).expect("reopen database");
    let handle = worker.handle();
    assert_eq!(outputs(&handle), before, "conclusions survive reopening");
    assert_eq!(
        wait(handle.job_configuration(check)).expect("read configuration"),
        Some(submitted)
    );
    assert_eq!(
        wait(handle.profile(llm)).expect("read Profile"),
        Some(declaration)
    );
    worker.shutdown().expect("shutdown");
    assert_relationships_consistent(&directory);
}

#[test]
fn frozen_inputs_stay_immutable_and_a_retry_is_a_new_job() {
    let (_root, directory) = data_directory("retry");
    let worker = DatabaseWorker::start(&directory).expect("start database");
    let handle = worker.handle();
    let genesis = handle.genesis();
    let (alpha, beta) = (project(&handle, "alpha"), project(&handle, "beta"));
    let parent = create_job(&handle, new_job("parent", genesis, Vec::new()));
    let draft = JobInputDraft {
        objective: Some("summarize the report".into()),
        ..JobInputDraft::default()
    };
    let input = wait(handle.save_job_input(None, draft.clone())).expect("save input");
    let original =
        wait(handle.create_job_with_input(new_job("original", parent, vec![alpha]), input))
            .expect("create original");

    let frozen = wait(handle.job_input(input))
        .expect("read input")
        .expect("input exists");
    assert!(frozen.submitted_at.is_some());
    assert_eq!(
        violation(wait(handle.save_job_input(
            Some(input),
            JobInputDraft {
                objective: Some("changed".into()),
                ..draft.clone()
            }
        ))),
        Violation::InputAlreadySubmitted(input)
    );
    assert_eq!(
        violation(wait(handle.create_job_with_input(
            new_job("reuse", parent, Vec::new()),
            input
        ))),
        Violation::InputAlreadySubmitted(input),
        "only a retry of its Job may run with a frozen input"
    );
    wait(handle.finalize_job(
        original,
        JobFinalization::Failed(result("first try failed")),
    ))
    .expect("finalize original");
    let original_record = wait(handle.job(original))
        .expect("read original")
        .expect("original exists");
    let original_outputs =
        wait(handle.job_outputs(original, None, 100)).expect("read original outputs");

    let retry = |input, projects| JobRetry {
        original,
        input,
        name: Some("second try".into()),
        execution_mode: ExecutionMode::Once,
        eligible_at: None,
        scheduled_at: None,
        next_due_at: None,
        next_iteration_no: None,
        iteration_no: None,
        projects,
        dependencies: Vec::new(),
        configuration: None,
        assigned_executor: None,
        acknowledge_repeated_effects: false,
    };
    let unchanged = wait(handle.retry_job(retry(RetryInput::Unchanged, vec![beta])))
        .expect("retry with the frozen input");
    wait(handle.finalize_job(
        unchanged,
        JobFinalization::Failed(result("second try failed")),
    ))
    .expect("settle the retry before another one");
    let unchanged_record = wait(handle.job(unchanged))
        .expect("read retry")
        .expect("retry exists");
    assert_ne!(unchanged, original);
    assert_eq!(
        unchanged_record.input, input,
        "the unchanged input is shared, not copied"
    );
    assert_eq!(
        unchanged_record.parent, parent,
        "the parent is derived from the original"
    );
    assert_eq!(unchanged_record.retry_of, Some(original));
    assert_eq!(
        unchanged_record.projects,
        [beta],
        "a retry need not keep the original's Project set"
    );

    assert_eq!(
        violation(wait(
            handle.retry_job(retry(RetryInput::Changed(input), Vec::new()))
        )),
        Violation::InputAlreadySubmitted(input)
    );
    let changed_input = wait(handle.save_job_input(
        None,
        JobInputDraft {
            objective: Some("summarize section 2".into()),
            ..draft
        },
    ))
    .expect("save changed input");
    let changed = wait(handle.retry_job(retry(RetryInput::Changed(changed_input), Vec::new())))
        .expect("retry with a new input");
    assert_eq!(
        wait(handle.job(changed))
            .expect("read retry")
            .expect("retry exists")
            .input,
        changed_input
    );
    assert!(matches!(
        violation(wait(
            handle.save_job_input(Some(changed_input), JobInputDraft::default())
        )),
        Violation::InputAlreadySubmitted(_)
    ));

    assert_eq!(
        wait(handle.job(original)).expect("read original"),
        Some(original_record),
        "the original Job is untouched"
    );
    assert_eq!(
        wait(handle.job_outputs(original, None, 100)).expect("read outputs"),
        original_outputs
    );
    assert_eq!(
        wait(handle.job_input(input)).expect("read input"),
        Some(frozen),
        "the frozen input is untouched"
    );
    assert_eq!(
        wait(handle.retries(original)).expect("read retries"),
        [unchanged, changed]
    );
    worker.shutdown().expect("shutdown");
    assert_relationships_consistent(&directory);
}

#[test]
fn outputs_keep_their_order_and_finalization_stays_atomic() {
    let (_root, directory) = data_directory("outputs");
    let worker = DatabaseWorker::start(&directory).expect("start database");
    let handle = worker.handle();
    let job = create_job(
        &handle,
        new_job("orchestrator", handle.genesis(), Vec::new()),
    );
    let child = create_job(&handle, new_job("worker", job, Vec::new()));
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

    assert_eq!(
        violation(wait(handle.finalize_job(
            job,
            JobFinalization::Completed(result("too early"))
        ))),
        Violation::UnfinishedChild(job)
    );
    let pending = wait(handle.job(job))
        .expect("read job")
        .expect("job exists");
    assert!(
        pending.completed_at.is_none() && pending.sterilized_at.is_none(),
        "a refused finalization changes nothing"
    );
    assert_eq!(
        wait(handle.job_outputs(job, None, 100))
            .expect("read outputs")
            .len(),
        2,
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
        (finalized.completed_at, finalized.sterilized_at.is_some()),
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
    let input = wait(handle.save_job_input(None, JobInputDraft::default())).expect("save input");
    assert_eq!(
        violation(wait(
            handle.create_job_with_input(new_job("late", job, Vec::new()), input)
        )),
        Violation::ParentClosed(job)
    );

    let unknown = create_job(&handle, new_job("unknown", handle.genesis(), Vec::new()));
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

fn executor(
    handle: &DatabaseHandle,
    name: &str,
    executor_type: ExecutorType,
    llm_transports: &[LlmTransport],
) -> Result<ExecutorId, DatabaseError> {
    wait(handle.create_executor(NewExecutor {
        name: name.into(),
        executor_type,
        concurrency_limit: NonZeroU32::MIN,
        llm_transports: llm_transports.iter().copied().collect(),
    }))
}

/// One Provider, two LLM Profiles and an Agent Profile that targets only the first.
struct Configured {
    provider: LlmProviderId,
    targeted: ProfileId,
    untargeted: ProfileId,
    agent: ProfileId,
}

fn configured(handle: &DatabaseHandle) -> Configured {
    let provider = provider(handle, "first");
    let targeted = llm_profile(
        handle,
        provider,
        llm_config("model-a", &[ModelCapability::Chat, ModelCapability::Tools]),
    );
    let untargeted = llm_profile(
        handle,
        provider,
        llm_config("model-b", &[ModelCapability::Chat]),
    );
    let agent = agent_profile(handle, "Answer briefly.", vec![targeted]);
    Configured {
        provider,
        targeted,
        untargeted,
        agent,
    }
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

#[test]
fn agent_profiles_target_llm_profiles_of_the_right_kind_only() {
    let (_root, directory) = data_directory("llm-targets");
    let worker = DatabaseWorker::start(&directory).expect("start database");
    let handle = worker.handle();
    let provider = provider(&handle, "first");
    let first = llm_profile(
        &handle,
        provider,
        llm_config("model-a", &[ModelCapability::Chat]),
    );
    let second = llm_profile(
        &handle,
        provider,
        llm_config("model-b", &[ModelCapability::Chat]),
    );
    let agent = agent_profile(&handle, "Answer briefly.", vec![second, first]);

    let typed = wait(handle.agent_profile(agent))
        .expect("read Agent Profile")
        .expect("Agent Profile exists");
    assert_eq!(
        typed.links,
        [first, second],
        "targets read back as a set in ID order, not as a preference order"
    );
    assert_eq!(
        wait(handle.profile(first))
            .expect("read LLM Profile")
            .expect("LLM Profile exists")
            .llm_targets,
        Vec::<ProfileId>::new()
    );

    let other_agent = agent_profile(&handle, "Other.", Vec::new());
    let missing = ProfileId::new(9_999);
    let refused = [
        (
            vec![first, other_agent],
            Violation::WrongProfileKind(WrongProfileKind {
                profile: other_agent,
                expected: ProfileKind::Llm,
                found: ProfileKind::Agent,
            }),
        ),
        (
            vec![missing],
            Violation::NotFound(RecordRef::Profile(missing)),
        ),
        (
            vec![first, first],
            Violation::DuplicateReference(RecordRef::Profile(first)),
        ),
    ];
    let count = wait(handle.profiles(None, 100))
        .expect("read Profiles")
        .len();
    for (llm_targets, expected) in refused {
        let profile = NewProfile {
            name: "refused".into(),
            config: NewProfileConfig::Agent {
                config: AgentConfig::default(),
                llm_targets,
            },
        };
        assert_eq!(violation(wait(handle.create_profile(profile))), expected);
    }
    assert_eq!(
        wait(handle.profiles(None, 100))
            .expect("read Profiles")
            .len(),
        count,
        "refused Profiles left nothing behind"
    );
    worker.shutdown().expect("shutdown");
    assert_relationships_consistent(&directory);
}

#[test]
fn submission_binds_input_parent_and_configuration_together_or_not_at_all() {
    let (_root, directory) = data_directory("submission");
    let worker = DatabaseWorker::start(&directory).expect("start database");
    let handle = worker.handle();
    let Configured {
        provider,
        targeted,
        untargeted,
        agent,
    } = configured(&handle);
    let llm_executor =
        executor(&handle, "llm worker", ExecutorType::Llm, &[OPENAI]).expect("register Executor");
    let other_executor =
        executor(&handle, "no adapter", ExecutorType::Llm, &[]).expect("register Executor");
    let parent = create_job(
        &handle,
        new_job("conversation", handle.genesis(), Vec::new()),
    );
    let input = wait(handle.save_job_input(
        None,
        JobInputDraft {
            objective: Some("answer".into()),
            ..JobInputDraft::default()
        },
    ))
    .expect("save input");
    let missing = ProfileId::new(9_999);
    let child = |configuration, assigned_executor| NewJob {
        configuration,
        assigned_executor,
        ..new_job("model request", parent, Vec::new())
    };
    let refused = [
        (
            child(configuration(agent, untargeted), None),
            Violation::NotAConfiguredTarget {
                agent_profile: agent,
                llm_profile: untargeted,
            },
        ),
        (
            child(configuration(targeted, targeted), None),
            Violation::WrongProfileKind(WrongProfileKind {
                profile: targeted,
                expected: ProfileKind::Agent,
                found: ProfileKind::Llm,
            }),
        ),
        (
            child(configuration(agent, missing), None),
            Violation::NotFound(RecordRef::Profile(missing)),
        ),
        (
            child(configuration(agent, targeted), Some(other_executor)),
            Violation::ExecutorLacksTransport {
                executor: other_executor,
                transport: OPENAI,
            },
        ),
    ];
    let jobs_before = wait(handle.jobs(None, 100)).expect("read jobs");
    for (job, expected) in refused {
        assert_eq!(
            violation(wait(handle.create_job_with_input(job, input))),
            expected
        );
    }
    assert_eq!(
        wait(handle.jobs(None, 100)).expect("read jobs"),
        jobs_before,
        "no partial child was committed"
    );
    assert!(
        wait(handle.children(parent, None, 100))
            .expect("read children")
            .is_empty()
    );
    assert!(
        wait(handle.profile_jobs(agent, None, 100))
            .expect("read Profile Jobs")
            .is_empty()
    );
    assert!(
        wait(handle.profile_jobs(targeted, None, 100))
            .expect("read Profile Jobs")
            .is_empty()
    );
    assert_eq!(
        wait(handle.job_input(input))
            .expect("read input")
            .expect("input exists")
            .submitted_at,
        None,
        "the input is not frozen"
    );

    let request = wait(handle.create_job_with_input(
        child(configuration(agent, targeted), Some(llm_executor)),
        input,
    ))
    .expect("submit child");
    let record = wait(handle.job(request))
        .expect("read child")
        .expect("child exists");
    assert_eq!(
        (record.input, record.parent, record.assigned_executor),
        (input, parent, Some(llm_executor))
    );
    assert_eq!(record.configuration, configuration(agent, targeted));
    assert_eq!(
        wait(handle.profile_jobs(agent, None, 100)).expect("read Profile Jobs"),
        [request]
    );
    assert_eq!(
        wait(handle.profile_jobs(targeted, None, 100)).expect("read Profile Jobs"),
        [request]
    );
    assert!(
        wait(handle.job(parent))
            .expect("read parent")
            .expect("parent exists")
            .configuration
            .is_none(),
        "orchestrating work needs no LLM Profile"
    );
    assert!(
        wait(handle.job(handle.genesis()))
            .expect("read Genesis")
            .expect("Genesis exists")
            .configuration
            .is_none()
    );

    let effective = wait(handle.job_configuration(request))
        .expect("read configuration")
        .expect("child is configured");
    assert_eq!(
        (
            effective.agent_profile,
            effective.llm_profile,
            effective.frozen.llm.provider.id
        ),
        (Some(agent), targeted, provider)
    );
    assert_eq!(
        effective
            .frozen
            .agent
            .as_ref()
            .and_then(|agent| agent.instructions.as_deref()),
        Some("Answer briefly.")
    );
    assert_eq!(effective.frozen.llm.config.model, "model-a");
    assert_eq!(
        effective.frozen.llm.config.parameters.temperature,
        Some(0.2)
    );
    assert_eq!(
        effective
            .frozen
            .llm
            .config
            .declared_capabilities
            .iter()
            .copied()
            .collect::<Vec<_>>(),
        [ModelCapability::Chat, ModelCapability::Tools]
    );
    assert_eq!(
        wait(handle.job_configuration(parent)).expect("read configuration"),
        None
    );
    worker.shutdown().expect("shutdown");
    assert_relationships_consistent(&directory);

    let worker = DatabaseWorker::start(&directory).expect("reopen database");
    let handle = worker.handle();
    assert_eq!(
        wait(handle.job_configuration(request)).expect("read configuration"),
        Some(effective),
        "the effective configuration survives reopening"
    );
    worker.shutdown().expect("shutdown");
}

#[test]
fn using_a_profile_never_changes_it() {
    let (_root, directory) = data_directory("immutable");
    let worker = DatabaseWorker::start(&directory).expect("start database");
    let handle = worker.handle();
    let Configured {
        targeted,
        untargeted,
        agent,
        ..
    } = configured(&handle);
    let read_profiles = |handle: &DatabaseHandle| {
        [agent, targeted, untargeted].map(|profile| {
            wait(handle.profile(profile))
                .expect("read Profile")
                .expect("Profile exists")
        })
    };
    let before = read_profiles(&handle);

    // Everything that uses, measures or observes the Profiles leaves them as created.
    let llm_executor =
        executor(&handle, "worker", ExecutorType::Llm, &[OPENAI]).expect("register Executor");
    wait(handle.create_agent(NewAgent {
        name: "helper".into(),
        profiles: vec![agent],
    }))
    .expect("create Agent");
    let job = create_job(
        &handle,
        NewJob {
            configuration: configuration(agent, targeted),
            ..new_job("request", handle.genesis(), Vec::new())
        },
    );
    wait(handle.assign_job_executor(job, llm_executor)).expect("assign Executor");
    let failed = provider_failure("invalid API key", Some(401), false);
    wait(handle.record_availability_stat(NewAvailabilityStat {
        job,
        request: RequestNumber::FIRST,
        observed_at: at(1_000),
        observation: failure(failed),
    }))
    .expect("record AvailabilityStat");
    wait(handle.record_metrics(NewMetrics {
        token: None,
        job,
        request: RequestNumber::FIRST,
        observed_at: at(1_000),
        measurements: Measurements {
            request_finished_in_ms: Some(40),
            ..Measurements::default()
        },
    }))
    .expect("record Metrics");
    wait(handle.finalize_job(job, JobFinalization::Cancelled(result("stopped"))))
        .expect("stop the work");
    assert_eq!(read_profiles(&handle), before);
    worker.shutdown().expect("shutdown");

    let worker = DatabaseWorker::start(&directory).expect("reopen database");
    assert_eq!(
        read_profiles(&worker.handle()),
        before,
        "using a Profile does not edit its payload, Provider or LLM targets"
    );
    worker.shutdown().expect("shutdown");
}

#[test]
fn one_llm_executor_runs_different_providers_and_models_exactly_as_frozen() {
    let (_root, directory) = data_directory("executors");
    let worker = DatabaseWorker::start(&directory).expect("start database");
    let handle = worker.handle();
    let genesis = handle.genesis();
    let (first_provider, second_provider) =
        (provider(&handle, "first"), provider(&handle, "second"));
    let model_a = llm_profile(
        &handle,
        first_provider,
        llm_config("model-a", &[ModelCapability::Chat]),
    );
    let model_b = llm_profile(
        &handle,
        second_provider,
        LlmConfig {
            parameters: LlmParameters {
                temperature: Some(0.9),
                top_p: Some(0.5),
                max_tokens: Some(64),
                ..LlmParameters::default()
            },
            ..llm_config("model-b", &[ModelCapability::Chat])
        },
    );
    let agent = agent_profile(&handle, "Answer briefly.", vec![model_a, model_b]);

    // Registration names transports, never Profiles, Providers or models.
    assert_eq!(
        violation(executor(&handle, "tool", ExecutorType::Tool, &[OPENAI])),
        Violation::InvalidValue(
            "llm_transports",
            "only an Llm Executor can run LLM transports"
        )
    );
    let shared =
        executor(&handle, "shared", ExecutorType::Llm, &[OPENAI]).expect("register Executor");
    let no_adapter =
        executor(&handle, "no adapter", ExecutorType::Llm, &[]).expect("register Executor");
    let tool = executor(&handle, "tool", ExecutorType::Tool, &[]).expect("register tool Executor");
    let registration = wait(handle.executor(shared))
        .expect("read Executor")
        .expect("Executor exists");
    assert_eq!(registration.llm_transports, BTreeSet::from([OPENAI]));
    for profile in [model_a, model_b] {
        assert_eq!(
            wait(handle.compatible_executors(profile)).expect("read candidates"),
            [shared]
        );
    }
    assert!(
        wait(handle.compatible_executors(agent))
            .expect("read candidates")
            .is_empty(),
        "an Agent Profile has no transport of its own"
    );

    let on_a = create_job(
        &handle,
        NewJob {
            configuration: configuration(agent, model_a),
            assigned_executor: Some(shared),
            ..new_job("request a", genesis, Vec::new())
        },
    );
    let on_b = create_job(
        &handle,
        NewJob {
            configuration: llm_only(model_b),
            assigned_executor: Some(shared),
            ..new_job("request b", genesis, Vec::new())
        },
    );
    let pending = create_job(
        &handle,
        NewJob {
            configuration: configuration(agent, model_b),
            ..new_job("pending", genesis, Vec::new())
        },
    );
    let unassigned = wait(handle.job(pending))
        .expect("read job")
        .expect("job exists");
    assert_eq!(
        violation(wait(handle.assign_job_executor(pending, no_adapter))),
        Violation::ExecutorLacksTransport {
            executor: no_adapter,
            transport: OPENAI
        }
    );
    assert_eq!(
        violation(wait(handle.assign_job_executor(pending, tool))),
        Violation::IncompatibleExecutorKind {
            executor: tool,
            found: ExecutorType::Tool
        }
    );
    let jobs_before = wait(handle.jobs(None, 100)).expect("read jobs");
    let input = wait(handle.save_job_input(None, JobInputDraft::default())).expect("save input");
    assert!(matches!(
        violation(wait(handle.create_job_with_input(
            NewJob {
                configuration: llm_only(model_a),
                assigned_executor: Some(no_adapter),
                ..new_job("refused", genesis, Vec::new())
            },
            input
        ))),
        Violation::ExecutorLacksTransport { .. }
    ));
    assert_eq!(
        wait(handle.jobs(None, 100)).expect("read jobs"),
        jobs_before
    );
    assert_eq!(
        wait(handle.job(pending)).expect("read job"),
        Some(unassigned),
        "refused assignments changed nothing"
    );
    wait(handle.assign_job_executor(pending, shared)).expect("assign the shared Executor");
    let unconfigured = create_job(&handle, new_job("system work", genesis, Vec::new()));
    wait(handle.assign_job_executor(unconfigured, tool))
        .expect("work without an LLM configuration keeps any Executor");

    let frozen_values = |handle: &DatabaseHandle, job| {
        let frozen = wait(handle.job_configuration(job))
            .expect("read configuration")
            .expect("job is configured")
            .frozen
            .llm;
        (
            frozen.provider.id,
            frozen.config.model,
            frozen.config.parameters,
        )
    };
    let a_parameters = llm_config("model-a", &[]).parameters;
    let b_parameters = LlmParameters {
        temperature: Some(0.9),
        top_p: Some(0.5),
        max_tokens: Some(64),
        ..LlmParameters::default()
    };
    for (job, expected) in [
        (
            on_a,
            (first_provider, "model-a".to_owned(), a_parameters.clone()),
        ),
        (
            on_b,
            (second_provider, "model-b".to_owned(), b_parameters.clone()),
        ),
        (
            pending,
            (second_provider, "model-b".to_owned(), b_parameters),
        ),
    ] {
        assert_eq!(
            frozen_values(&handle, job),
            expected,
            "{job} runs exactly what it froze"
        );
        assert_eq!(
            wait(handle.job(job))
                .expect("read job")
                .expect("job exists")
                .assigned_executor,
            Some(shared)
        );
        assert_eq!(
            wait(handle.job_compatible_executors(job)).expect("read candidates"),
            [shared]
        );
    }

    // Changed request parameters reach later work on the same Executor
    // without a new registration.
    let history = [on_a, on_b, pending]
        .map(|job| wait(handle.job_configuration(job)).expect("read configuration"));
    for job in [on_a, on_b, pending] {
        wait(handle.finalize_job(job, JobFinalization::Completed(result("done")))).expect("finish");
    }
    let changed = LlmParameters {
        temperature: Some(1.0),
        max_tokens: Some(1_024),
        ..LlmParameters::default()
    };
    wait(handle.update_profile(
        model_a,
        NewProfile {
            name: "model-a".into(),
            config: NewProfileConfig::Llm {
                provider: first_provider,
                config: LlmConfig {
                    parameters: changed.clone(),
                    ..llm_config("model-a", &[ModelCapability::Chat])
                },
            },
        },
    ))
    .expect("edit request parameters");
    let later = create_job(
        &handle,
        NewJob {
            configuration: configuration(agent, model_a),
            assigned_executor: Some(shared),
            ..new_job("later request", genesis, Vec::new())
        },
    );
    assert_eq!(
        frozen_values(&handle, later),
        (first_provider, "model-a".to_owned(), changed)
    );
    assert_eq!(
        [on_a, on_b, pending]
            .map(|job| wait(handle.job_configuration(job)).expect("read configuration")),
        history,
        "earlier work keeps what it froze"
    );
    worker.shutdown().expect("shutdown");

    let worker = DatabaseWorker::start(&directory).expect("reopen database");
    let handle = worker.handle();
    assert_eq!(
        [on_a, on_b, pending]
            .map(|job| wait(handle.job_configuration(job)).expect("read configuration")),
        history
    );
    assert_eq!(
        wait(handle.executor(shared)).expect("read Executor"),
        Some(registration)
    );
    worker.shutdown().expect("shutdown");
    assert_relationships_consistent(&directory);
}

#[test]
fn the_execution_runtime_runs_a_configured_child_only_on_a_compatible_executor() {
    let (_root, directory) = data_directory("runtime");
    let worker = DatabaseWorker::start(&directory).expect("start database");
    let handle = worker.handle();
    let Configured {
        targeted, agent, ..
    } = configured(&handle);
    let compatible =
        executor(&handle, "llm worker", ExecutorType::Llm, &[OPENAI]).expect("register Executor");
    let incompatible =
        executor(&handle, "no adapter", ExecutorType::Llm, &[]).expect("register Executor");
    let runtime = ExecutionRuntime::start(handle.clone()).expect("start runtime");
    let plan = |executor_id| ExecutionPlan {
        executor_id,
        input: JobInputDraft {
            objective: Some("answer".into()),
            ..JobInputDraft::default()
        },
        job: NewJob {
            configuration: configuration(agent, targeted),
            ..new_job("model request", handle.genesis(), Vec::new())
        },
        outcome: JobOutcome::Completed,
        result: result("answered"),
    };
    let jobs_before = wait(handle.jobs(None, 100)).expect("read jobs").len();
    match wait(runtime.scheduler().schedule(plan(incompatible))) {
        Err(ExecutionRuntimeError::Database(DatabaseError::Rejected(violation))) => {
            assert_eq!(
                violation,
                Violation::ExecutorLacksTransport {
                    executor: incompatible,
                    transport: OPENAI
                }
            )
        }
        other => panic!("an incompatible Executor must be refused, got {other:?}"),
    }
    assert_eq!(
        wait(handle.jobs(None, 100)).expect("read jobs").len(),
        jobs_before,
        "the refused plan created no Job"
    );

    let receipt =
        wait(runtime.scheduler().schedule(plan(compatible))).expect("run on a compatible Executor");
    let record = wait(handle.job(receipt.job))
        .expect("read job")
        .expect("job exists");
    assert_eq!(
        (record.assigned_executor, record.configuration),
        (Some(compatible), configuration(agent, targeted))
    );
    assert!(record.completed_at.is_some());
    runtime.shutdown().expect("shutdown runtime");
    worker.shutdown().expect("shutdown");
}

#[test]
fn retries_share_one_frozen_input_without_overwriting_it() {
    let (_root, directory) = data_directory("shared-input");
    let worker = DatabaseWorker::start(&directory).expect("start database");
    let handle = worker.handle();
    let input = wait(handle.save_job_input(
        None,
        JobInputDraft {
            objective: Some("summarize".into()),
            ..JobInputDraft::default()
        },
    ))
    .expect("save input");
    let original = wait(
        handle.create_job_with_input(new_job("original", handle.genesis(), Vec::new()), input),
    )
    .expect("create original");
    let retry = |original| JobRetry {
        original,
        input: RetryInput::Unchanged,
        name: None,
        execution_mode: ExecutionMode::Once,
        eligible_at: None,
        scheduled_at: None,
        next_due_at: None,
        next_iteration_no: None,
        iteration_no: None,
        projects: Vec::new(),
        dependencies: Vec::new(),
        configuration: None,
        assigned_executor: None,
        acknowledge_repeated_effects: false,
    };
    let fail = |job| {
        wait(handle.finalize_job(job, JobFinalization::Failed(result("failed"))))
            .expect("settle before retrying");
    };
    fail(original);
    let first = wait(handle.retry_job(retry(original))).expect("retry");
    fail(first);
    let of_retry = wait(handle.retry_job(retry(first))).expect("retry the retry");
    let second = wait(handle.retry_job(retry(original))).expect("retry again");

    assert_eq!(
        wait(handle.input_jobs(input)).expect("read input Jobs"),
        [original, first, of_retry, second]
    );
    for job in [original, first, of_retry, second] {
        assert_eq!(
            wait(handle.job(job))
                .expect("read job")
                .expect("job exists")
                .input,
            input,
            "{job} keeps the shared input"
        );
    }
    assert_eq!(
        wait(handle.job(of_retry))
            .expect("read job")
            .expect("job exists")
            .retry_of,
        Some(first)
    );
    assert_eq!(
        wait(handle.retries(original)).expect("read retries"),
        [first, second]
    );
    assert_eq!(
        wait(handle.retries(first)).expect("read retries"),
        [of_retry]
    );
    worker.shutdown().expect("shutdown");
    assert_relationships_consistent(&directory);
}

fn llm_edit(provider: LlmProviderId, model: &str) -> NewProfile {
    NewProfile {
        name: model.into(),
        config: NewProfileConfig::Llm {
            provider,
            config: llm_config(model, &[ModelCapability::Chat]),
        },
    }
}

fn agent_edit(instructions: &str, llm_targets: Vec<ProfileId>) -> NewProfile {
    NewProfile {
        name: "assistant".into(),
        config: NewProfileConfig::Agent {
            config: AgentConfig {
                instructions: Some(instructions.into()),
            },
            llm_targets,
        },
    }
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
            ..new_job("model request", handle.genesis(), Vec::new())
        },
    )
}

fn blocking(handle: &DatabaseHandle, profile: ProfileId) -> Vec<JobId> {
    let editability = wait(handle.profile_editability(profile))
        .expect("read editability")
        .expect("Profile exists");
    assert_eq!(editability.editable(), editability.blocking_jobs.is_empty());
    editability.blocking_jobs
}

fn frozen_model(handle: &DatabaseHandle, job: JobId) -> String {
    wait(handle.job_configuration(job))
        .expect("read configuration")
        .expect("job is configured")
        .frozen
        .llm
        .config
        .model
}

#[test]
fn unlocked_profiles_are_edited_with_their_links_and_rejected_edits_change_nothing() {
    let (_root, directory) = data_directory("edit");
    let worker = DatabaseWorker::start(&directory).expect("start database");
    let handle = worker.handle();
    let Configured {
        provider: first_provider,
        targeted,
        untargeted,
        agent,
    } = configured(&handle);
    let second_provider = provider(&handle, "second");
    let other_agent = agent_profile(&handle, "Other.", Vec::new());

    wait(handle.update_profile(targeted, llm_edit(second_provider, "model-c")))
        .expect("edit an unlocked LLM Profile");
    wait(handle.update_profile(
        agent,
        agent_edit("Answer in detail.", vec![untargeted, targeted]),
    ))
    .expect("edit an unlocked Agent Profile");
    let edited_llm = wait(handle.llm_profile(targeted))
        .expect("read LLM Profile")
        .expect("LLM Profile exists");
    assert_eq!(
        (edited_llm.config.model.as_str(), edited_llm.links),
        ("model-c", second_provider),
        "identity kept, payload and Provider replaced"
    );
    assert!(edited_llm.updated_at >= edited_llm.created_at);
    let edited_agent = wait(handle.agent_profile(agent))
        .expect("read Agent Profile")
        .expect("Agent Profile exists");
    assert_eq!(
        edited_agent.config.instructions.as_deref(),
        Some("Answer in detail.")
    );
    assert_eq!(edited_agent.links, [targeted, untargeted]);
    assert_eq!(
        wait(handle.provider_profiles(first_provider)).expect("read provider Profiles"),
        [untargeted]
    );
    assert_eq!(
        wait(handle.provider_profiles(second_provider)).expect("read provider Profiles"),
        [targeted]
    );

    let missing_profile = ProfileId::new(9_999);
    let missing_provider = LlmProviderId::new(9_999);
    let refused = [
        (
            agent,
            llm_edit(first_provider, "model-d"),
            Violation::WrongProfileKind(WrongProfileKind {
                profile: agent,
                expected: ProfileKind::Agent,
                found: ProfileKind::Llm,
            }),
        ),
        (
            agent,
            agent_edit("x", vec![other_agent]),
            Violation::WrongProfileKind(WrongProfileKind {
                profile: other_agent,
                expected: ProfileKind::Llm,
                found: ProfileKind::Agent,
            }),
        ),
        (
            agent,
            agent_edit("x", vec![missing_profile]),
            Violation::NotFound(RecordRef::Profile(missing_profile)),
        ),
        (
            agent,
            agent_edit("x", vec![untargeted, untargeted]),
            Violation::DuplicateReference(RecordRef::Profile(untargeted)),
        ),
        (
            targeted,
            llm_edit(missing_provider, "model-d"),
            Violation::NotFound(RecordRef::LlmProvider(missing_provider)),
        ),
        (
            targeted,
            llm_edit(first_provider, ""),
            Violation::InvalidValue("model", "cannot be empty"),
        ),
        (
            missing_profile,
            llm_edit(first_provider, "model-d"),
            Violation::NotFound(RecordRef::Profile(missing_profile)),
        ),
    ];
    let snapshot = |handle: &DatabaseHandle| {
        (
            wait(handle.profiles(None, 100)).expect("read Profiles"),
            wait(handle.provider_profiles(first_provider)).expect("read provider Profiles"),
            wait(handle.provider_profiles(second_provider)).expect("read provider Profiles"),
        )
    };
    let before = snapshot(&handle);
    for (profile, edit, expected) in refused {
        assert_eq!(
            violation(wait(handle.update_profile(profile, edit))),
            expected
        );
    }
    assert_eq!(
        snapshot(&handle),
        before,
        "refused edits left payloads, links and indexes alone"
    );
    worker.shutdown().expect("shutdown");
    assert_relationships_consistent(&directory);
}

#[test]
fn unfinished_work_locks_both_profiles_until_the_last_holder_finishes() {
    let (_root, directory) = data_directory("locks");
    let worker = DatabaseWorker::start(&directory).expect("start database");
    let handle = worker.handle();
    type Finish = fn() -> JobFinalization;
    let outcomes: [(&str, Finish); 3] = [
        ("completed", || JobFinalization::Completed(result("done"))),
        ("failed", || JobFinalization::Failed(result("broken"))),
        ("cancelled", || {
            JobFinalization::Cancelled(result("stopped"))
        }),
    ];
    for (name, finish) in outcomes {
        let Configured {
            provider,
            targeted,
            untargeted,
            agent,
        } = configured(&handle);
        // Idle uses hold nothing: Agent membership and Executor registration.
        wait(handle.create_agent(NewAgent {
            name: "helper".into(),
            profiles: vec![agent],
        }))
        .expect("create Agent");
        executor(&handle, "worker", ExecutorType::Llm, &[OPENAI]).expect("register Executor");
        assert!(
            blocking(&handle, agent).is_empty() && blocking(&handle, targeted).is_empty(),
            "{name}: idle Profiles are editable"
        );

        let first = configured_child(&handle, agent, targeted);
        let second = configured_child(&handle, agent, targeted);
        for profile in [agent, targeted] {
            assert_eq!(
                blocking(&handle, profile),
                [first, second],
                "{name}: queued work holds {profile}"
            );
        }
        assert!(
            blocking(&handle, untargeted).is_empty(),
            "{name}: an unchosen target is not held"
        );
        assert_eq!(
            violation(wait(
                handle.update_profile(agent, agent_edit("changed", vec![targeted]))
            )),
            Violation::ProfileLocked {
                profile: agent,
                blocking_jobs: vec![first, second]
            }
        );
        assert!(matches!(
            violation(wait(
                handle.update_profile(targeted, llm_edit(provider, "model-z"))
            )),
            Violation::ProfileLocked { .. }
        ));

        wait(handle.start_job(first)).expect("start");
        wait(handle.finalize_job(first, finish())).expect("finish the first holder");
        assert_eq!(
            blocking(&handle, targeted),
            [second],
            "{name}: one finished Job does not unlock a shared Profile"
        );
        assert!(matches!(
            violation(wait(
                handle.update_profile(targeted, llm_edit(provider, "model-z"))
            )),
            Violation::ProfileLocked { .. }
        ));

        wait(handle.finalize_job(second, finish())).expect("finish the last holder");
        assert!(
            blocking(&handle, agent).is_empty() && blocking(&handle, targeted).is_empty(),
            "{name}: the last finished holder unlocks"
        );
        wait(handle.update_profile(targeted, llm_edit(provider, "model-z")))
            .expect("edit after unlock");
        wait(handle.update_profile(agent, agent_edit("changed", vec![targeted])))
            .expect("edit after unlock");
    }

    worker.shutdown().expect("shutdown");
    assert_relationships_consistent(&directory);
}

#[test]
fn edits_and_submissions_take_effect_in_admission_order() {
    let (_root, directory) = data_directory("ordering");
    let worker = DatabaseWorker::start(&directory).expect("start database");
    let handle = worker.handle();
    let Configured {
        provider,
        targeted,
        agent,
        ..
    } = configured(&handle);
    let submission = |input| {
        handle.create_job_with_input(
            NewJob {
                configuration: configuration(agent, targeted),
                ..new_job("request", handle.genesis(), Vec::new())
            },
            input,
        )
    };

    // Edit first: the submission queued behind it freezes the new values.
    let input = wait(handle.save_job_input(None, JobInputDraft::default())).expect("save input");
    let edit = admit(handle.update_profile(targeted, llm_edit(provider, "model-new")));
    let submit = admit(submission(input));
    edit().expect("the edit runs first");
    let after_edit = submit().expect("the submission runs second");
    assert_eq!(frozen_model(&handle, after_edit), "model-new");

    // Submit first: the edit queued behind it finds the Profile held.
    let input = wait(handle.save_job_input(None, JobInputDraft::default())).expect("save input");
    let submit = admit(submission(input));
    let edit = admit(handle.update_profile(targeted, llm_edit(provider, "model-newer")));
    let before_edit = submit().expect("the submission runs first");
    assert!(matches!(violation(edit()), Violation::ProfileLocked { .. }));
    assert_eq!(frozen_model(&handle, before_edit), "model-new");
    assert_eq!(
        wait(handle.llm_profile(targeted))
            .expect("read")
            .expect("exists")
            .config
            .model,
        "model-new"
    );
    worker.shutdown().expect("shutdown");
}

#[test]
fn snapshots_and_locks_survive_reopening_and_later_edits_reach_only_later_work() {
    let (_root, directory) = data_directory("snapshots");
    let worker = DatabaseWorker::start(&directory).expect("start database");
    let handle = worker.handle();
    let Configured {
        provider,
        targeted,
        agent,
        ..
    } = configured(&handle);
    let earlier = configured_child(&handle, agent, targeted);
    let earlier_configuration = wait(handle.job_configuration(earlier))
        .expect("read configuration")
        .expect("configured");
    let earlier_input = wait(handle.job(earlier))
        .expect("read job")
        .expect("job exists")
        .input;
    assert_eq!(
        wait(handle.job_input(earlier_input))
            .expect("read input")
            .expect("input exists")
            .configuration
            .as_ref(),
        Some(&earlier_configuration.frozen),
        "the snapshot lives in the frozen input"
    );
    assert_eq!(
        earlier_configuration
            .frozen
            .llm
            .provider
            .credential_ref
            .as_deref(),
        Some("FIRST_KEY"),
        "the credential is named, not copied"
    );
    worker.shutdown().expect("shutdown");

    let worker = DatabaseWorker::start(&directory).expect("reopen database");
    let handle = worker.handle();
    assert_eq!(
        blocking(&handle, targeted),
        [earlier],
        "the derived lock survives reopening"
    );
    wait(handle.finalize_job(earlier, JobFinalization::Completed(result("done")))).expect("finish");
    wait(handle.update_profile(targeted, llm_edit(provider, "model-b"))).expect("edit LLM Profile");
    wait(handle.update_profile(agent, agent_edit("Answer in detail.", vec![targeted])))
        .expect("edit Agent Profile");
    let later = configured_child(&handle, agent, targeted);
    let later_configuration = wait(handle.job_configuration(later))
        .expect("read configuration")
        .expect("configured");
    assert_eq!(later_configuration.frozen.llm.config.model, "model-b");
    assert_eq!(
        later_configuration
            .frozen
            .agent
            .as_ref()
            .and_then(|agent| agent.instructions.as_deref()),
        Some("Answer in detail.")
    );
    assert_eq!(
        wait(handle.job_configuration(earlier)).expect("read configuration"),
        Some(earlier_configuration.clone()),
        "the earlier Job keeps what it froze"
    );
    assert_eq!(
        (
            later_configuration.agent_profile,
            later_configuration.llm_profile
        ),
        (Some(agent), targeted),
        "provenance names the same Profiles"
    );
    worker.shutdown().expect("shutdown");

    let worker = DatabaseWorker::start(&directory).expect("reopen database");
    let handle = worker.handle();
    assert_eq!(
        wait(handle.job_configuration(earlier)).expect("read configuration"),
        Some(earlier_configuration)
    );
    assert_eq!(
        wait(handle.job_configuration(later)).expect("read configuration"),
        Some(later_configuration)
    );
    assert_eq!(blocking(&handle, targeted), [later]);
    worker.shutdown().expect("shutdown");
    assert_relationships_consistent(&directory);
}

#[test]
fn an_unknown_outcome_holds_its_profiles_until_its_stop_is_confirmed() {
    let (_root, directory) = data_directory("unknown");
    let worker = DatabaseWorker::start(&directory).expect("start database");
    let handle = worker.handle();
    let genesis = handle.genesis();
    let Configured {
        provider,
        targeted,
        agent,
        ..
    } = configured(&handle);
    let running = configured_child(&handle, agent, targeted);
    assert_eq!(
        violation(wait(handle.confirm_job_stopped(running))),
        Violation::StopNeedsUnknownOutcome(running),
        "work without an unknown outcome has nothing to confirm"
    );
    let unknown = configured_child(&handle, agent, targeted);
    let child = create_job(&handle, new_job("helper", unknown, Vec::new()));
    wait(handle.start_job(unknown)).expect("start");
    wait(handle.finalize_job(unknown, JobFinalization::Unknown)).expect("record unknown outcome");
    wait(handle.finalize_job(running, JobFinalization::Completed(result("done")))).expect("finish");
    for profile in [agent, targeted] {
        assert_eq!(
            blocking(&handle, profile),
            [unknown],
            "an unknown outcome may still be executing"
        );
    }
    let recorded = wait(handle.job(unknown))
        .expect("read job")
        .expect("job exists");
    assert_eq!(recorded.stop_confirmed_at, None);
    worker.shutdown().expect("shutdown");

    // A restart alone is no proof that the execution stopped.
    let worker = DatabaseWorker::start(&directory).expect("reopen database");
    let handle = worker.handle();
    assert_eq!(blocking(&handle, targeted), [unknown]);

    // The confirmation is ordered with edits on the write worker.
    let early_edit = admit(handle.update_profile(targeted, llm_edit(provider, "model-z")));
    let confirm = admit(handle.confirm_job_stopped(unknown));
    let late_edit = admit(handle.update_profile(targeted, llm_edit(provider, "model-z")));
    assert_eq!(
        violation(early_edit()),
        Violation::ProfileLocked {
            profile: targeted,
            blocking_jobs: vec![unknown]
        }
    );
    confirm().expect("the trusted path confirms the stop");
    late_edit().expect("the edit queued after the confirmation applies");
    assert!(blocking(&handle, agent).is_empty());
    assert_eq!(
        violation(wait(handle.confirm_job_stopped(unknown))),
        Violation::StopAlreadyConfirmed(unknown)
    );

    let confirmed = wait(handle.job(unknown))
        .expect("read job")
        .expect("job exists");
    assert!(confirmed.stop_confirmed_at.is_some());
    assert_eq!(
        (
            confirmed.unknown_at,
            confirmed.completed_at,
            confirmed.failed_at,
            confirmed.cancelled_at
        ),
        (recorded.unknown_at, None, None, None),
        "the outcome stays unknown"
    );
    assert!(
        wait(handle.job_outputs(unknown, None, 10))
            .expect("read outputs")
            .is_empty(),
        "no result is published"
    );

    // The stopped Job's own execution is over, so a success dependency on it
    // can never be met and the dependent can fail but never complete. Its
    // child still runs, though, so its subtree has not settled: the stopped
    // Job still waits for that child and the dependency edge stays, and a
    // cycle closed through them is refused.
    let dependent = create_job(
        &handle,
        NewJob {
            dependencies: vec![(unknown, DependencyCondition::Success)],
            ..new_job("needs success", genesis, Vec::new())
        },
    );
    let input = wait(handle.save_job_input(None, JobInputDraft::default())).expect("save input");
    assert!(
        matches!(
            violation(wait(handle.create_job_with_input(
                NewJob {
                    dependencies: vec![(dependent, DependencyCondition::Success)],
                    ..new_job(
                        "closed a cycle through a stopped Job with an active child",
                        child,
                        Vec::new(),
                    )
                },
                input,
            ))),
            Violation::WaitingCycle(_)
        ),
        "a waiting edge runs through a stopped Job while its subtree is active"
    );
    assert_eq!(
        violation(wait(handle.finalize_job(
            dependent,
            JobFinalization::Completed(result("done"))
        ))),
        Violation::UnsatisfiedDependency {
            job: dependent,
            prerequisite: unknown,
            state: DependencyState::Impossible
        }
    );
    wait(handle.finalize_job(dependent, JobFinalization::Failed(result("cannot proceed"))))
        .expect("an impossible prerequisite does not block failure");
    worker.shutdown().expect("shutdown");

    let worker = DatabaseWorker::start(&directory).expect("reopen database");
    let handle = worker.handle();
    assert!(
        blocking(&handle, targeted).is_empty(),
        "the confirmation survives reopening"
    );
    assert_eq!(
        wait(handle.job(unknown)).expect("read job"),
        Some(confirmed)
    );
    worker.shutdown().expect("shutdown");
    assert_relationships_consistent(&directory);
}

#[test]
fn an_idle_service_holds_nothing_and_each_execution_holds_what_it_froze() {
    let (_root, directory) = data_directory("service");
    let worker = DatabaseWorker::start(&directory).expect("start database");
    let handle = worker.handle();
    let Configured {
        provider,
        targeted,
        agent,
        ..
    } = configured(&handle);
    let service = create_job(
        &handle,
        NewJob {
            execution_mode: ExecutionMode::Continuous,
            ..new_job("service", handle.genesis(), Vec::new())
        },
    );
    wait(handle.start_job(service)).expect("start the service");
    assert!(
        blocking(&handle, agent).is_empty() && blocking(&handle, targeted).is_empty(),
        "a service waiting for triggers holds nothing"
    );
    wait(handle.update_profile(targeted, llm_edit(provider, "model-b")))
        .expect("edit while only the service exists");

    let execution = |configuration| {
        create_job(
            &handle,
            NewJob {
                configuration,
                ..new_job("triggered execution", service, Vec::new())
            },
        )
    };
    let first = execution(configuration(agent, targeted));
    let second = execution(llm_only(targeted));
    assert_eq!(
        blocking(&handle, targeted),
        [first, second],
        "queued executions share the LLM Profile"
    );
    assert_eq!(blocking(&handle, agent), [first]);
    assert!(matches!(
        violation(wait(
            handle.update_profile(targeted, llm_edit(provider, "model-c"))
        )),
        Violation::ProfileLocked { .. }
    ));

    wait(handle.finalize_job(first, JobFinalization::Completed(result("done")))).expect("finish");
    wait(handle.update_profile(agent, agent_edit("Answer in detail.", vec![targeted])))
        .expect("the Agent Profile is released by its last holder");
    assert_eq!(
        blocking(&handle, targeted),
        [second],
        "the remaining holder keeps the LLM Profile"
    );
    wait(handle.finalize_job(second, JobFinalization::Failed(result("broken")))).expect("finish");
    wait(handle.update_profile(targeted, llm_edit(provider, "model-c")))
        .expect("edit after the last holder");

    let third = execution(configuration(agent, targeted));
    assert_eq!(frozen_model(&handle, third), "model-c");
    assert_eq!(
        wait(handle.job_configuration(third))
            .expect("read configuration")
            .and_then(|submitted| submitted.frozen.agent)
            .and_then(|agent| agent.instructions),
        Some("Answer in detail.".into())
    );
    for job in [first, second] {
        assert_eq!(
            frozen_model(&handle, job),
            "model-b",
            "{job} keeps what it froze"
        );
    }
    assert_eq!(
        wait(handle.job_configuration(service)).expect("read configuration"),
        None
    );
    assert_eq!(
        wait(handle.profile_jobs(targeted, None, 100)).expect("read Profile Jobs"),
        [first, second, third]
    );
    worker.shutdown().expect("shutdown");

    let worker = DatabaseWorker::start(&directory).expect("reopen database");
    assert_eq!(blocking(&worker.handle(), targeted), [third]);
    worker.shutdown().expect("shutdown");
    assert_relationships_consistent(&directory);
}

#[test]
fn metrics_and_availability_keep_raw_facts_about_the_llm_their_job_froze() {
    let (_root, directory) = data_directory("observations");
    let worker = DatabaseWorker::start(&directory).expect("start database");
    let handle = worker.handle();
    let Configured {
        provider,
        targeted,
        untargeted,
        agent,
    } = configured(&handle);
    let metrics = |job, observed_at, measurements| {
        wait(handle.record_metrics(NewMetrics {
            token: None,
            job,
            request: RequestNumber::FIRST,
            observed_at: at(observed_at),
            measurements,
        }))
    };
    let observe = |job, observed_at, observation| {
        wait(handle.record_availability_stat(NewAvailabilityStat {
            job,
            request: RequestNumber::FIRST,
            observed_at: at(observed_at),
            observation,
        }))
    };

    // A request that got an answer; only some token counts were reported.
    let answered = configured_child(&handle, agent, targeted);
    let answered_metrics = metrics(
        answered,
        1_000,
        Measurements {
            ttfb_ms: Some(120),
            output_ttft_ms: Some(130),
            request_finished_in_ms: Some(250),
            usage: NormalizedUsage {
                input_tokens: Some(12),
                ..NormalizedUsage::default()
            },
            ..Measurements::default()
        },
    )
    .expect("record Metrics");
    let answered_stat = observe(answered, 1_000, response(200)).expect("record AvailabilityStat");
    // A Job may be measured any number of times.
    let answered_again = metrics(
        answered,
        1_500,
        Measurements {
            request_finished_in_ms: Some(90),
            ..Measurements::default()
        },
    )
    .expect("record more Metrics");

    // Provider failures are kept as the transport reported them, and one Job
    // may make any number of observed requests.
    let unauthorized = provider_failure("invalid API key", Some(401), false);
    let timed_out = provider_failure("the request timed out", None, true);
    let refused_by_provider = configured_child(&handle, agent, targeted);
    let unauthorized_stat = observe(refused_by_provider, 2_000, failure(unauthorized.clone()))
        .expect("record AvailabilityStat");
    let timed_out_stat = observe(refused_by_provider, 3_000, failure(timed_out.clone()))
        .expect("record AvailabilityStat");

    // A request refused locally before it was sent, and a cancellation
    // before any response, keep their Job facts and real measurements but
    // make no availability observation.
    let rejected_locally = configured_child(&handle, agent, targeted);
    let local_metrics = metrics(
        rejected_locally,
        4_000,
        Measurements {
            request_finished_in_ms: Some(1),
            ..Measurements::default()
        },
    )
    .expect("record Metrics");
    wait(handle.finalize_job(
        rejected_locally,
        JobFinalization::Failed(FinalResult {
            code: Some("REQUEST_REJECTED_LOCALLY".into()),
            ..result("the request was invalid")
        }),
    ))
    .expect("finish");
    let cancelled = configured_child(&handle, agent, targeted);
    wait(handle.finalize_job(
        cancelled,
        JobFinalization::Cancelled(result("stopped by the user")),
    ))
    .expect("finish");
    wait(handle.finalize_job(answered, JobFinalization::Completed(result("answered"))))
        .expect("finish");
    wait(handle.finalize_job(
        refused_by_provider,
        JobFinalization::Failed(result("unavailable")),
    ))
    .expect("finish");

    // Metrics are generic; work without an LLM can be measured, but it makes
    // no LLM observation.
    let orchestrator = create_job(
        &handle,
        new_job("orchestrator", handle.genesis(), Vec::new()),
    );
    let orchestrator_metrics = metrics(
        orchestrator,
        5_000,
        Measurements {
            request_finished_in_ms: Some(7),
            ..Measurements::default()
        },
    )
    .expect("record Metrics");
    let missing = JobId::new(9_999);
    let markers_before = wait(handle.operation_markers(None, 1_000)).expect("read markers");
    assert_eq!(
        violation(observe(orchestrator, 5_000, response(200))),
        Violation::NotAnLlmExecution(orchestrator)
    );
    assert_eq!(
        violation(observe(missing, 5_000, response(200))),
        Violation::NotFound(RecordRef::Job(missing))
    );
    assert_eq!(
        violation(metrics(
            missing,
            5_000,
            Measurements {
                ttfb_ms: Some(1),
                ..Measurements::default()
            }
        )),
        Violation::NotFound(RecordRef::Job(missing))
    );
    assert_eq!(
        violation(observe(
            answered,
            5_000,
            AvailabilityObservation {
                status_code: None,
                finish_reason: None,
                raw_finish_reason: None,
                failure: None
            }
        )),
        Violation::InvalidValue("observation", "records no response or failure")
    );
    assert_eq!(
        violation(metrics(answered, 5_000, Measurements::default())),
        Violation::InvalidValue("measurements", "records no measurement")
    );
    assert_eq!(
        wait(handle.operation_markers(None, 1_000)).expect("read markers"),
        markers_before,
        "refused observations committed nothing"
    );

    let read = |handle: &DatabaseHandle| {
        (
            all_metrics(handle, &of_profile(targeted), 100),
            all_availability(handle, &of_profile(targeted), 100),
        )
    };
    let (llm_metrics, availability) = read(&handle);
    assert_eq!(
        llm_metrics
            .iter()
            .map(|record| (record.id, record.job))
            .collect::<Vec<_>>(),
        [
            (answered_metrics, answered),
            (answered_again, answered),
            (local_metrics, rejected_locally)
        ]
    );
    assert_eq!(
        availability
            .iter()
            .map(|record| (record.id, record.job))
            .collect::<Vec<_>>(),
        [
            (answered_stat, answered),
            (unauthorized_stat, refused_by_provider),
            (timed_out_stat, refused_by_provider)
        ],
        "only requests that were observed count, whatever the Jobs' outcomes"
    );
    for job in [rejected_locally, cancelled] {
        assert!(
            wait(handle.job_availability(job, None, 100))
                .expect("read AvailabilityStats")
                .is_empty(),
            "{job} made no availability observation"
        );
    }
    let usage = &llm_metrics[0].measurements.usage;
    assert_eq!(
        (
            usage.input_tokens,
            usage.output_tokens,
            usage.reasoning_tokens
        ),
        (Some(12), None, None),
        "unreported token counts stay missing, not zero"
    );
    assert_eq!(llm_metrics[0].measurements.reasoning_ttft_ms, None);
    assert_eq!(
        (
            availability[1].observation.failure.as_ref(),
            availability[2].observation.failure.as_ref()
        ),
        (Some(&unauthorized), Some(&timed_out))
    );
    let frozen = LlmTarget {
        llm_profile: targeted,
        frozen: wait(handle.job_configuration(answered))
            .expect("read configuration")
            .expect("configured")
            .frozen
            .llm,
    };
    assert!(availability.iter().all(|record| record.target == frozen));
    assert!(
        llm_metrics
            .iter()
            .all(|record| record.target.as_ref() == Some(&frozen))
    );
    assert_eq!(
        wait(handle.job_metrics(orchestrator, None, 100))
            .expect("read Metrics")
            .into_iter()
            .map(|record| (record.id, record.target))
            .collect::<Vec<_>>(),
        [(orchestrator_metrics, None)]
    );
    assert!(all_availability(&handle, &of_profile(untargeted), 100).is_empty());

    // Time ranges include `from`, exclude `until`, and page by position.
    let ids = |records: Vec<AvailabilityStatRecord>| {
        records
            .into_iter()
            .map(|record| record.id)
            .collect::<Vec<_>>()
    };
    let window = ObservationFilter {
        llm_profile: Some(targeted),
        ..observed_between(2_000, 3_000)
    };
    assert_eq!(
        ids(all_availability(&handle, &window, 100)),
        [unauthorized_stat]
    );
    let first =
        wait(handle.availability_stats(of_profile(targeted), None, 2)).expect("read first page");
    assert_eq!(
        first.next,
        Some(ObservationCursor {
            observed_at: at(2_000),
            record: unauthorized_stat
        })
    );
    assert_eq!(ids(first.records), [answered_stat, unauthorized_stat]);
    let second = wait(handle.availability_stats(of_profile(targeted), first.next, 2))
        .expect("read next page");
    assert_eq!(
        (ids(second.records), second.next),
        (vec![timed_out_stat], None)
    );

    // Every holder has finished, so the Profile can be edited; history stays
    // with the configuration its Jobs froze.
    wait(handle.update_profile(targeted, llm_edit(provider, "model-b"))).expect("edit LLM Profile");
    let later = configured_child(&handle, agent, targeted);
    let later_stat = observe(later, 6_000, response(200)).expect("record AvailabilityStat");
    let (metrics_after, availability_after) = read(&handle);
    assert_eq!(metrics_after, llm_metrics);
    assert_eq!(
        availability_after[..3],
        availability[..],
        "earlier observations keep model-a"
    );
    assert_eq!(
        (
            availability_after[3].id,
            availability_after[3].target.frozen.config.model.as_str()
        ),
        (later_stat, "model-b")
    );
    worker.shutdown().expect("shutdown");

    let worker = DatabaseWorker::start(&directory).expect("reopen database");
    assert_eq!(
        read(&worker.handle()),
        (metrics_after, availability_after),
        "observations survive reopening unchanged"
    );
    worker.shutdown().expect("shutdown");
    assert_relationships_consistent(&directory);
}

#[test]
fn observation_queries_select_frozen_targets_in_stable_bounded_pages() {
    let (_root, directory) = data_directory("observation-queries");
    let worker = DatabaseWorker::start(&directory).expect("start database");
    let handle = worker.handle();
    let genesis = handle.genesis();
    let (first, second) = (provider(&handle, "first"), provider(&handle, "second"));
    let a = llm_profile(
        &handle,
        first,
        llm_config("model-x", &[ModelCapability::Chat]),
    );
    let b = llm_profile(
        &handle,
        second,
        llm_config("model-x", &[ModelCapability::Chat]),
    );
    let c = llm_profile(
        &handle,
        first,
        llm_config("model-y", &[ModelCapability::Chat]),
    );
    let llm_job = |profile| {
        create_job(
            &handle,
            NewJob {
                configuration: llm_only(profile),
                ..new_job("request", genesis, Vec::new())
            },
        )
    };
    let (on_a, on_b, on_c) = (llm_job(a), llm_job(b), llm_job(c));
    let plain = create_job(&handle, new_job("system work", genesis, Vec::new()));
    let observe = |job, observed_at| {
        wait(handle.record_availability_stat(NewAvailabilityStat {
            job,
            request: RequestNumber::FIRST,
            observed_at: at(observed_at),
            observation: response(200),
        }))
        .expect("record AvailabilityStat")
    };
    let measure = |job, observed_at| {
        wait(handle.record_metrics(NewMetrics {
            token: None,
            job,
            request: RequestNumber::FIRST,
            observed_at: at(observed_at),
            measurements: Measurements {
                request_finished_in_ms: Some(10),
                ..Measurements::default()
            },
        }))
        .expect("record Metrics")
    };

    // Recording order differs from observation order, and several
    // observations share a time.
    let (s1, s2, s3) = (observe(on_a, 100), observe(on_a, 100), observe(on_a, 200));
    let s4 = observe(on_b, 100);
    let s6 = observe(on_c, 150);
    let (m1, m2, m3, m4) = (
        measure(on_a, 100),
        measure(plain, 100),
        measure(on_c, 150),
        measure(on_a, 200),
    );

    // Cancelling a Job neither erases an observation it already obtained
    // nor refuses one obtained earlier and reported after the cancellation.
    wait(handle.finalize_job(
        on_b,
        JobFinalization::Cancelled(result("stopped by the user")),
    ))
    .expect("cancel");
    let s5 = observe(on_b, 300);
    assert_eq!(
        wait(handle.job_availability(on_b, None, 100))
            .expect("read AvailabilityStats")
            .into_iter()
            .map(|record| record.id)
            .collect::<Vec<_>>(),
        [s4, s5]
    );

    // Refused writes leave no record and no index entry behind.
    let markers_before = wait(handle.operation_markers(None, 1_000)).expect("read markers");
    assert_eq!(
        violation(wait(handle.record_availability_stat(NewAvailabilityStat {
            job: plain,
            request: RequestNumber::FIRST,
            observed_at: at(100),
            observation: response(200),
        }))),
        Violation::NotAnLlmExecution(plain)
    );
    assert_eq!(
        violation(wait(handle.record_metrics(NewMetrics {
            token: None,
            job: on_a,
            request: RequestNumber::FIRST,
            observed_at: at(100),
            measurements: Measurements::default(),
        }))),
        Violation::InvalidValue("measurements", "records no measurement")
    );
    assert_eq!(
        wait(handle.operation_markers(None, 1_000)).expect("read markers"),
        markers_before
    );

    // Edit `a` to another Provider and model once its work has finished.
    for job in [on_a, on_c] {
        wait(handle.finalize_job(job, JobFinalization::Completed(result("done")))).expect("finish");
    }
    wait(handle.update_profile(a, llm_edit(second, "model-z"))).expect("edit LLM Profile");
    let on_a_later = llm_job(a);
    let s7 = observe(on_a_later, 400);

    let filter = |llm_profile, provider, model: Option<&str>| ObservationFilter {
        llm_profile,
        provider,
        model: model.map(str::to_owned),
        ..all_time()
    };
    let cases: Vec<(&str, ObservationFilter, Vec<AvailabilityStatId>)> = vec![
        ("everything", all_time(), vec![s1, s2, s4, s6, s3, s5, s7]),
        (
            "first Provider",
            filter(None, Some(first), None),
            vec![s1, s2, s6, s3],
        ),
        (
            "second Provider",
            filter(None, Some(second), None),
            vec![s4, s5, s7],
        ),
        (
            "model-x",
            filter(None, None, Some("model-x")),
            vec![s1, s2, s4, s3, s5],
        ),
        ("model-z", filter(None, None, Some("model-z")), vec![s7]),
        (
            "first Provider and model-x",
            filter(None, Some(first), Some("model-x")),
            vec![s1, s2, s3],
        ),
        (
            "second Provider and model-x",
            filter(None, Some(second), Some("model-x")),
            vec![s4, s5],
        ),
        (
            "Profile a, across its edit",
            filter(Some(a), None, None),
            vec![s1, s2, s3, s7],
        ),
        (
            "Profile a on the second Provider",
            filter(Some(a), Some(second), None),
            vec![s7],
        ),
        (
            "Profile a with model-x",
            filter(Some(a), None, Some("model-x")),
            vec![s1, s2, s3],
        ),
        (
            "Profile a, second Provider, model-z",
            filter(Some(a), Some(second), Some("model-z")),
            vec![s7],
        ),
        (
            "Profile b on the first Provider",
            filter(Some(b), Some(first), None),
            vec![],
        ),
        (
            "an unknown model",
            filter(None, None, Some("model-unknown")),
            vec![],
        ),
        (
            "from 100 until 150",
            observed_between(100, 150),
            vec![s1, s2, s4],
        ),
        ("from 150 until 200", observed_between(150, 200), vec![s6]),
        ("an empty range", observed_between(100, 100), vec![]),
        ("a reversed range", observed_between(200, 100), vec![]),
        (
            "around the time the records were committed",
            observed_between(Timestamp::now().as_unix_micros() - 600_000_000, i64::MAX),
            vec![],
        ),
    ];
    let check = |handle: &DatabaseHandle| {
        for (name, filter, expected) in &cases {
            for page in [1, 2, 3, 1_000] {
                let found: Vec<_> = all_availability(handle, filter, page)
                    .into_iter()
                    .map(|record| record.id)
                    .collect();
                assert_eq!(&found, expected, "{name}, {page} per page");
            }
        }
        let empty =
            wait(handle.availability_stats(filter(None, None, Some("model-unknown")), None, 10))
                .expect("read");
        assert_eq!(
            (empty.records.len(), empty.next),
            (0, None),
            "an empty result has no continuation"
        );
        let records = all_availability(handle, &filter(Some(a), None, None), 10);
        assert_eq!(
            records
                .iter()
                .map(|record| (
                    record.target.frozen.provider.id,
                    record.target.frozen.config.model.as_str()
                ))
                .collect::<Vec<_>>(),
            [
                (first, "model-x"),
                (first, "model-x"),
                (first, "model-x"),
                (second, "model-z")
            ],
            "records keep the target their Job froze"
        );
        assert!(
            records
                .iter()
                .all(|record| record.recorded_at > record.observed_at),
            "the committed time is kept apart from the observation time"
        );
        let metric_ids = |filter: ObservationFilter| {
            all_metrics(handle, &filter, 2)
                .into_iter()
                .map(|record| record.id)
                .collect::<Vec<_>>()
        };
        assert_eq!(
            metric_ids(all_time()),
            [m1, m2, m3, m4],
            "Metrics without an LLM are listed too"
        );
        assert_eq!(
            metric_ids(filter(None, Some(first), None)),
            [m1, m3, m4],
            "but match no LLM filter"
        );
        assert_eq!(metric_ids(filter(None, None, Some("model-x"))), [m1, m4]);
        assert_eq!(
            wait(handle.job_metrics(plain, None, 10))
                .expect("read Metrics")
                .into_iter()
                .map(|record| (record.id, record.target))
                .collect::<Vec<_>>(),
            [(m2, None)]
        );
        assert_eq!(
            wait(handle.job_metrics(on_a, None, 10))
                .expect("read Metrics")
                .into_iter()
                .map(|record| record.id)
                .collect::<Vec<_>>(),
            [m1, m4]
        );
    };
    check(&handle);
    worker.shutdown().expect("shutdown");
    assert_relationships_consistent(&directory);

    let worker = DatabaseWorker::start(&directory).expect("reopen database");
    check(&worker.handle());
    worker.shutdown().expect("shutdown");

    // A filter the scanned scope cannot narrow reads a bounded number of
    // entries per call and continues even after a segment with no match.
    let opened = crate::storage::open(directory.database_path())
        .unwrap_or_else(|_| panic!("open the closed database file"));
    let read = |filter: &ObservationFilter, after, scan_limit| {
        crate::storage::read_availability_stats(&opened.database, filter, after, 100, scan_limit)
            .expect("read")
    };
    let on_second = filter(Some(a), Some(second), None);
    let page = read(&on_second, None, 2);
    assert_eq!(
        (page.records.len(), page.next),
        (
            0,
            Some(ObservationCursor {
                observed_at: at(100),
                record: s2
            })
        ),
        "two entries of Profile a were read and neither matched"
    );
    let page = read(&on_second, page.next, 2);
    assert_eq!(
        (
            page.records
                .into_iter()
                .map(|record| record.id)
                .collect::<Vec<_>>(),
            page.next
        ),
        (vec![s7], None)
    );
    let mut after = None;
    let mut found = Vec::new();
    loop {
        let page = read(&filter(Some(a), None, Some("model-x")), after, 1);
        found.extend(page.records.into_iter().map(|record| record.id));
        match page.next {
            Some(next) => after = Some(next),
            None => break,
        }
    }
    assert_eq!(found, [s1, s2, s3]);
}

#[test]
fn observed_model_choices_come_from_the_frozen_targets_of_actual_observations() {
    use crate::persistence::{
        ObservedModel, ObservedModelChoice, ObservedModelFilter, ObservedModelPage,
    };

    let (_root, directory) = data_directory("observed-models");
    let worker = DatabaseWorker::start(&directory).expect("start database");
    let handle = worker.handle();
    let genesis = handle.genesis();
    let (first, second) = (provider(&handle, "first"), provider(&handle, "second"));
    let profile = |provider, model: &str| {
        llm_profile(
            &handle,
            provider,
            llm_config(model, &[ModelCapability::Chat]),
        )
    };
    // The same name at two Providers, a name another one starts with, a
    // name differing only in case, two Profiles of one pair, and a
    // configured model that is never observed.
    let (a, b, c, d, f) = (
        profile(first, "model"),
        profile(second, "model"),
        profile(first, "model-x"),
        profile(first, "Model"),
        profile(first, "model"),
    );
    profile(second, "unobserved");
    let llm_job = |profile| {
        create_job(
            &handle,
            NewJob {
                configuration: llm_only(profile),
                ..new_job("request", genesis, Vec::new())
            },
        )
    };
    let (on_a, on_b, on_c, on_d, on_f) =
        (llm_job(a), llm_job(b), llm_job(c), llm_job(d), llm_job(f));
    let plain = create_job(&handle, new_job("system work", genesis, Vec::new()));
    let observe = |job, observed_at| {
        wait(handle.record_availability_stat(NewAvailabilityStat {
            job,
            request: RequestNumber::FIRST,
            observed_at: at(observed_at),
            observation: response(200),
        }))
        .expect("record AvailabilityStat")
    };
    let measure = |job, observed_at| {
        wait(handle.record_metrics(NewMetrics {
            token: None,
            job,
            request: RequestNumber::FIRST,
            observed_at: at(observed_at),
            measurements: Measurements {
                request_finished_in_ms: Some(10),
                ..Measurements::default()
            },
        }))
        .expect("record Metrics")
    };
    for offset in 0..30 {
        observe(on_a, 100 + offset);
    }
    measure(on_f, 500);
    measure(on_b, 200);
    observe(on_c, 300);
    measure(on_d, 50);
    measure(plain, 100);
    // Once `a` is edited to another Provider and model, its old target
    // stays a choice beside the new one.
    wait(handle.finalize_job(on_a, JobFinalization::Completed(result("done")))).expect("finish");
    wait(handle.update_profile(a, llm_edit(second, "model-z"))).expect("edit LLM Profile");
    observe(llm_job(a), 600);

    let pair = |provider, model: &str| ObservedModel {
        provider,
        model: model.into(),
    };
    let choice = |provider, model: &str, has_metrics, has_availability_stats| ObservedModelChoice {
        model: pair(provider, model),
        has_metrics,
        has_availability_stats,
    };
    let filter = |from, until, provider| ObservedModelFilter {
        observed: observed_between(from, until).observed,
        provider,
    };
    let everything = filter(i64::MIN, i64::MAX, None);
    let all_choices = |handle: &DatabaseHandle, filter: ObservedModelFilter, page: usize| {
        let (mut choices, mut after) = (Vec::new(), None::<ObservedModel>);
        for _ in 0..1_000 {
            let ObservedModelPage {
                choices: found,
                next,
            } = wait(handle.observed_models(filter, after.clone(), page)).expect("read choices");
            choices.extend(found);
            match next {
                None => return choices,
                Some(next) => {
                    assert!(
                        after.is_none_or(|after| next > after),
                        "the cursor must advance"
                    );
                    after = Some(next);
                }
            }
        }
        panic!("the query did not finish");
    };
    let cases = [
        (
            "everything",
            everything,
            vec![
                choice(first, "Model", true, false),
                choice(first, "model", true, true),
                choice(first, "model-x", false, true),
                choice(second, "model", true, false),
                choice(second, "model-z", false, true),
            ],
        ),
        (
            "the second Provider",
            filter(i64::MIN, i64::MAX, Some(second)),
            vec![
                choice(second, "model", true, false),
                choice(second, "model-z", false, true),
            ],
        ),
        (
            "from 100 until 300",
            filter(100, 300, None),
            vec![
                choice(first, "model", false, true),
                choice(second, "model", true, false),
            ],
        ),
        (
            "from 300 until 301",
            filter(300, 301, None),
            vec![choice(first, "model-x", false, true)],
        ),
        (
            "the first Provider from 500",
            filter(500, i64::MAX, Some(first)),
            vec![choice(first, "model", true, false)],
        ),
        ("an empty range", filter(300, 300, None), vec![]),
        ("a reversed range", filter(300, 100, None), vec![]),
        (
            "a Provider with no observations",
            filter(i64::MIN, i64::MAX, Some(LlmProviderId::new(9_999))),
            vec![],
        ),
    ];
    let check = |handle: &DatabaseHandle| {
        for (name, filter, expected) in &cases {
            for page in [1, 2, 1_000] {
                assert_eq!(
                    &all_choices(handle, *filter, page),
                    expected,
                    "{name}, {page} per page"
                );
            }
        }
        let empty =
            wait(handle.observed_models(filter(300, 300, None), None, 10)).expect("read choices");
        assert_eq!(
            empty,
            ObservedModelPage {
                choices: Vec::new(),
                next: None
            }
        );
        // A cursor outside the Provider filter neither widens nor breaks it.
        let page = wait(handle.observed_models(
            filter(i64::MIN, i64::MAX, Some(second)),
            Some(pair(first, "Model")),
            10,
        ))
        .expect("read choices");
        assert_eq!(page.choices.len(), 2);
        let page = wait(handle.observed_models(
            filter(i64::MIN, i64::MAX, Some(first)),
            Some(pair(second, "model")),
            10,
        ))
        .expect("read choices");
        assert_eq!(
            page,
            ObservedModelPage {
                choices: Vec::new(),
                next: None
            }
        );
    };
    check(&handle);
    worker.shutdown().expect("shutdown");
    assert_relationships_consistent(&directory);

    let worker = DatabaseWorker::start(&directory).expect("reopen database");
    check(&worker.handle());
    worker.shutdown().expect("shutdown");

    // The scan bound counts pairs, not records: thirty records of one pair
    // cost one examined pair, and pairs outside the range still advance.
    let opened = crate::storage::open(directory.database_path())
        .unwrap_or_else(|_| panic!("open the closed database file"));
    let read = |filter, after: Option<&ObservedModel>| {
        crate::storage::read_observed_models(&opened.database, &filter, after, 10, 1)
            .expect("read choices")
    };
    let page = read(everything, Some(&pair(first, "Model")));
    assert_eq!(
        page,
        ObservedModelPage {
            choices: vec![choice(first, "model", true, true)],
            next: Some(pair(first, "model"))
        }
    );
    let late = filter(600, i64::MAX, None);
    let mut pages = vec![read(late, None)];
    while let Some(next) = pages.last().and_then(|page| page.next.clone()) {
        pages.push(read(late, Some(&next)));
    }
    assert_eq!(pages.len(), 5, "one examined pair per call");
    assert!(
        pages[..4]
            .iter()
            .all(|page| page.choices.is_empty() && page.next.is_some())
    );
    assert_eq!(pages[4].choices, [choice(second, "model-z", false, true)]);
}

#[test]
fn statistics_target_lookup_reads_no_project_associations() {
    use crate::persistence::ObservedModelFilter;

    let (_root, directory) = data_directory("target-lookup");
    let worker = DatabaseWorker::start(&directory).expect("start database");
    let handle = worker.handle();
    let genesis = handle.genesis();
    let projects: Vec<_> = (0..3)
        .map(|index| project(&handle, &format!("project {index}")))
        .collect();
    let llm_provider = provider(&handle, "first");
    let llm = llm_profile(
        &handle,
        llm_provider,
        llm_config("model-a", &[ModelCapability::Chat]),
    );
    let configured = create_job(
        &handle,
        NewJob {
            configuration: llm_only(llm),
            ..new_job("request", genesis, projects.clone())
        },
    );
    let plain = create_job(&handle, new_job("system work", genesis, projects));
    for job in [configured, plain] {
        wait(handle.record_metrics(NewMetrics {
            token: None,
            job,
            request: RequestNumber::FIRST,
            observed_at: at(100),
            measurements: Measurements {
                request_finished_in_ms: Some(10),
                ..Measurements::default()
            },
        }))
        .expect("record Metrics");
    }
    wait(handle.record_availability_stat(NewAvailabilityStat {
        job: configured,
        request: RequestNumber::FIRST,
        observed_at: at(100),
        observation: response(200),
    }))
    .expect("record AvailabilityStat");

    let on_provider = ObservationFilter {
        provider: Some(llm_provider),
        ..all_time()
    };
    let everything = ObservedModelFilter {
        observed: all_time().observed,
        provider: None,
    };
    let read = |handle: &DatabaseHandle| {
        (
            all_metrics(handle, &all_time(), 10),
            all_metrics(handle, &on_provider, 10),
            all_availability(handle, &all_time(), 10),
            wait(handle.job_metrics(configured, None, 10)).expect("read Metrics"),
            wait(handle.job_metrics(plain, None, 10)).expect("read Metrics"),
            wait(handle.job_availability(configured, None, 10)).expect("read AvailabilityStats"),
            wait(handle.observed_models(everything, None, 10)).expect("read choices"),
        )
    };
    let before = read(&handle);
    assert_eq!((before.0.len(), before.1.len(), before.2.len()), (2, 1, 1));
    assert_eq!(
        before.4[0].target, None,
        "work without an LLM keeps no target"
    );

    // Any Project traversal now fails; a Job-detail read proves the probe is armed.
    wait(handle.with_database(crate::storage::break_project_links)).expect("arm the probe");
    assert!(
        matches!(wait(handle.job(configured)), Err(DatabaseError::Read(_))),
        "a Job-detail read traverses Projects and must trip the probe"
    );
    assert_eq!(
        read(&handle),
        before,
        "statistics reads resolve targets without Projects"
    );

    // The configured Job's frozen target is still required.
    wait(
        handle
            .with_database(move |database| crate::storage::remove_input_link(database, configured)),
    )
    .expect("remove the input link");
    fn corrupted<T>(result: Result<T, DatabaseError>) -> bool {
        matches!(result, Err(DatabaseError::Read(error)) if error.kind == crate::database::StorageErrorKind::Corrupted)
    }
    assert!(corrupted(wait(handle.metrics(all_time(), None, 10))));
    assert!(corrupted(wait(handle.availability_stats(
        all_time(),
        None,
        10
    ))));
    assert!(corrupted(wait(handle.job_metrics(configured, None, 10))));
    assert_eq!(
        wait(handle.job_metrics(plain, None, 10)).expect("read Metrics"),
        before.4,
        "a Job without an LLM needs no input to be measured"
    );
    worker.shutdown().expect("shutdown");
}

#[test]
fn job_observation_reads_are_empty_without_records_and_corrupt_only_when_dangling() {
    let (_root, directory) = data_directory("job-observation-reads");
    let worker = DatabaseWorker::start(&directory).expect("start database");
    let handle = worker.handle();
    let Configured {
        targeted, agent, ..
    } = configured(&handle);
    let unknown = JobId::new(9_999);
    let unobserved = configured_child(&handle, agent, targeted);
    let measured = configured_child(&handle, agent, targeted);
    wait(handle.record_metrics(NewMetrics {
        token: None,
        job: measured,
        request: RequestNumber::FIRST,
        observed_at: at(100),
        measurements: Measurements {
            request_finished_in_ms: Some(10),
            ..Measurements::default()
        },
    }))
    .expect("record Metrics");
    wait(handle.record_availability_stat(NewAvailabilityStat {
        job: measured,
        request: RequestNumber::FIRST,
        observed_at: at(100),
        observation: response(200),
    }))
    .expect("record AvailabilityStat");
    fn corrupted<T>(result: Result<T, DatabaseError>) -> bool {
        matches!(result, Err(DatabaseError::Read(error)) if error.kind == crate::database::StorageErrorKind::Corrupted)
    }
    let empty = |handle: &DatabaseHandle, job| {
        wait(handle.job_metrics(job, None, 10))
            .expect("read Metrics of a Job without records")
            .is_empty()
            && wait(handle.job_availability(job, None, 10))
                .expect("read AvailabilityStats of a Job without records")
                .is_empty()
    };

    assert!(
        empty(&handle, unknown),
        "an unknown Job has no records and reads an empty page"
    );
    assert!(
        empty(&handle, unobserved),
        "a configured Job without records reads an empty page"
    );

    // An empty page never resolves the frozen target, so a broken input is not read.
    wait(
        handle
            .with_database(move |database| crate::storage::remove_input_link(database, unobserved)),
    )
    .expect("remove the input link");
    assert!(
        empty(&handle, unobserved),
        "a Job without records needs no frozen target"
    );

    // Records whose Job row is gone are dangling references and must not read as empty.
    wait(handle.with_database(move |database| crate::storage::remove_job_row(database, measured)))
        .expect("remove the Job row");
    assert!(corrupted(wait(handle.job_metrics(measured, None, 10))));
    assert!(corrupted(wait(handle.job_availability(measured, None, 10))));
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
    let job = configured_child(handle, agent, targeted);
    wait(handle.start_job(job)).expect("start job");
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
    let executor =
        executor(&handle, "llm worker", ExecutorType::Llm, &[OPENAI]).expect("register Executor");
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
    let executor =
        executor(&handle, "llm worker", ExecutorType::Llm, &[OPENAI]).expect("register Executor");

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

#[test]
fn request_samples_refuse_mixed_jobs_and_report_no_sample_without_records() {
    let (_root, directory) = data_directory("request-sample-mixed");
    let worker = DatabaseWorker::start(&directory).expect("start database");
    let handle = worker.handle();
    let Configured {
        targeted, agent, ..
    } = configured(&handle);
    let first = configured_child(&handle, agent, targeted);
    let second = configured_child(&handle, agent, targeted);
    observe(&handle, first, 100, response(200));
    observe(&handle, second, 100, response(200));
    let (first_records, second_records) = (stats(&handle, first), stats(&handle, second));

    assert_eq!(
        AvailabilityRequestSample::from_records(first_records.iter().chain(&second_records)),
        Err(MixedRequests {
            first: RequestRef {
                job: first,
                request: RequestNumber::FIRST
            },
            other: RequestRef {
                job: second,
                request: RequestNumber::FIRST
            }
        }),
        "evidence from two Jobs is never one request"
    );
    assert_eq!(
        AvailabilityRequestSample::from_records(std::iter::empty::<&AvailabilityStatRecord>()),
        Ok(None),
        "no records is no sample"
    );
    worker.shutdown().expect("shutdown");
}

#[test]
fn a_retry_is_its_own_request_sample_even_with_a_shared_input() {
    let (_root, directory) = data_directory("request-sample-retry");
    let worker = DatabaseWorker::start(&directory).expect("start database");
    let handle = worker.handle();
    let Configured {
        targeted, agent, ..
    } = configured(&handle);
    let original = configured_child(&handle, agent, targeted);
    observe(
        &handle,
        original,
        100,
        failure(provider_failure("overloaded", Some(503), true)),
    );
    wait(handle.finalize_job(
        original,
        JobFinalization::Failed(result("first try failed")),
    ))
    .expect("finish");
    let input = wait(handle.job(original))
        .expect("read job")
        .expect("job exists")
        .input;
    let retry = wait(handle.retry_job(JobRetry {
        original,
        input: RetryInput::Unchanged,
        name: None,
        execution_mode: ExecutionMode::Once,
        eligible_at: None,
        scheduled_at: None,
        next_due_at: None,
        next_iteration_no: None,
        iteration_no: None,
        projects: Vec::new(),
        dependencies: Vec::new(),
        configuration: configuration(agent, targeted),
        assigned_executor: None,
        acknowledge_repeated_effects: false,
    }))
    .expect("retry with the same configuration");
    observe(&handle, retry, 200, response(200));

    let retry_input = wait(handle.job(retry))
        .expect("read job")
        .expect("job exists")
        .input;
    assert_eq!(retry_input, input, "the retry shares the frozen input");
    let original_sample = sample_of(&stats(&handle, original));
    let retry_sample = sample_of(&stats(&handle, retry));
    assert_eq!(
        (original_sample.job, original_sample.classification),
        (
            original,
            AvailabilityClassification::Unavailable(UnavailableCause::ProviderServerError)
        ),
        "the failed attempt keeps its own sample"
    );
    assert_eq!(
        (retry_sample.job, retry_sample.classification),
        (retry, AvailabilityClassification::Available),
        "the retry is a separate sample"
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

/// One LLM Provider and one LLM Profile on it, for jobs that run an LLM directly.
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
            ..new_job("request", handle.genesis(), Vec::new())
        },
    )
}

/// The micros of a time inside minute `minute`, a little after its start.
fn at_minute(minute: i64) -> i64 {
    minute * 60_000_000 + 1_000
}

/// One committed batch, which must not be idle.
fn consume_batch(handle: &DatabaseHandle, limit: usize) -> AvailabilityBatchReport {
    match wait(handle.consume_availability(limit)).expect("consume a batch") {
        AvailabilityBatch::Consumed(report) => report,
        other => panic!("expected a committed batch, got {other:?}"),
    }
}

/// Consumes until nothing is left, and returns how many records that took.
fn drain(handle: &DatabaseHandle) -> usize {
    let mut consumed = 0;
    loop {
        match wait(handle.consume_availability(usize::MAX)).expect("consume a batch") {
            AvailabilityBatch::Consumed(report) => consumed += report.consumed_records,
            AvailabilityBatch::Idle { .. } => return consumed,
            other => panic!("aggregation must not contend in a test, got {other:?}"),
        }
    }
}

/// The counts of one bucket as stored, or `None` when it holds no requests.
fn bucket(
    handle: &DatabaseHandle,
    provider: LlmProviderId,
    model: &str,
    micros: i64,
) -> Option<AvailabilityCounts> {
    wait(handle.availability_minute(provider, model.to_owned(), minute_of(at(micros))))
        .expect("read the minute")
        .value
}

fn counts(
    handle: &DatabaseHandle,
    provider: LlmProviderId,
    model: &str,
    micros: i64,
) -> AvailabilityCounts {
    bucket(handle, provider, model, micros).unwrap_or_default()
}

fn requests(counts: AvailabilityCounts) -> u64 {
    counts.available
        + counts.provider_server_error
        + counts.rate_limited
        + counts.auth_or_permission
        + counts.request_or_target
        + counts.unconfirmed
}

#[test]
fn aggregation_with_nothing_to_consume_writes_nothing() {
    let (_root, directory) = data_directory("aggregation-empty");
    let worker = DatabaseWorker::start(&directory).expect("start database");
    let handle = worker.handle();
    let (_provider, llm) = llm_setup(&handle, "model-a");
    assert_eq!(
        wait(handle.consume_availability(10)).expect("consume"),
        AvailabilityBatch::Idle {
            consumed_through: None
        },
        "no record means no batch"
    );
    assert_eq!(
        wait(handle.availability_progress()).expect("read progress"),
        AvailabilityAggregationProgress::default(),
        "an empty step commits no progress"
    );

    let job = llm_job(&handle, llm);
    observe(&handle, job, at_minute(1), response(200));
    let report = consume_batch(&handle, 10);
    let progress = wait(handle.availability_progress()).expect("read progress");
    assert_eq!(
        (progress.consumed_through, report.consumed_records),
        (Some(report.consumed_through), 1)
    );
    assert_eq!(
        wait(handle.consume_availability(10)).expect("consume"),
        AvailabilityBatch::Idle {
            consumed_through: Some(report.consumed_through)
        },
        "an idle step after a batch is idle"
    );
    assert_eq!(
        wait(handle.availability_progress())
            .expect("read progress")
            .updated_at,
        progress.updated_at,
        "an idle step writes no update time"
    );
    worker.shutdown().expect("shutdown");
}

#[test]
fn batches_consume_committed_records_in_id_order_within_the_limit() {
    let (_root, directory) = data_directory("aggregation-bounds");
    let worker = DatabaseWorker::start(&directory).expect("start database");
    let handle = worker.handle();
    let (provider, llm) = llm_setup(&handle, "model-a");
    let jobs: Vec<JobId> = (0..5).map(|_| llm_job(&handle, llm)).collect();
    for (index, job) in jobs.iter().enumerate() {
        observe(&handle, *job, at_minute(1) + index as i64, bare(200));
    }

    let sizes: Vec<usize> = (0..3)
        .map(|_| consume_batch(&handle, 2).consumed_records)
        .collect();
    assert_eq!(
        sizes,
        [2, 2, 1],
        "each batch takes at most its limit, in order"
    );
    let last = stats(&handle, jobs[4])[0].id;
    assert_eq!(
        wait(handle.consume_availability(2)).expect("consume"),
        AvailabilityBatch::Idle {
            consumed_through: Some(last)
        },
        "progress ends at the last committed record"
    );
    assert_eq!(
        counts(&handle, provider, "model-a", at_minute(1)).unconfirmed,
        5
    );
    worker.shutdown().expect("shutdown");
}

#[test]
fn a_request_split_across_batches_moves_only_its_own_contribution() {
    let (_root, directory) = data_directory("aggregation-split");
    let worker = DatabaseWorker::start(&directory).expect("start database");
    let handle = worker.handle();
    let (provider, llm) = llm_setup(&handle, "model-a");
    let split = llm_job(&handle, llm);
    let other = llm_job(&handle, llm);
    let micros = at_minute(2);

    observe(&handle, split, micros, bare(200));
    observe(&handle, other, micros + 1, response(200));
    observe(
        &handle,
        split,
        micros + 2,
        AvailabilityObservation {
            status_code: None,
            finish_reason: Some(ChatFinishReason::Stop),
            raw_finish_reason: Some("stop".into()),
            failure: None,
        },
    );
    assert_eq!(consume_batch(&handle, 2).consumed_records, 2);
    let first = counts(&handle, provider, "model-a", micros);
    assert_eq!(
        (first.available, first.unconfirmed, requests(first)),
        (1, 1, 2)
    );

    let report = consume_batch(&handle, 1);
    assert_eq!(
        (report.samples_written, report.buckets_written),
        (1, 1),
        "one request changes; its minute is rewritten once"
    );
    let completed = counts(&handle, provider, "model-a", micros);
    assert_eq!(
        (
            completed.available,
            completed.unconfirmed,
            requests(completed)
        ),
        (2, 0, 2)
    );

    observe(
        &handle,
        split,
        micros + 3,
        failure(provider_failure("overloaded", Some(503), true)),
    );
    consume_batch(&handle, 10);
    let contradicted = counts(&handle, provider, "model-a", micros);
    assert_eq!(
        (
            contradicted.available,
            contradicted.unconfirmed,
            requests(contradicted)
        ),
        (1, 1, 2),
        "a contradiction moves the same request back to Unconfirmed"
    );
    assert_eq!(
        stats(&handle, split).len(),
        3,
        "the raw history is kept as recorded"
    );
    worker.shutdown().expect("shutdown");
}

#[test]
fn each_request_counts_once_in_its_class_and_duplicates_add_nothing() {
    let (_root, directory) = data_directory("aggregation-classes");
    let worker = DatabaseWorker::start(&directory).expect("start database");
    let handle = worker.handle();
    let (provider, llm) = llm_setup(&handle, "model-a");
    let micros = at_minute(3);
    let duplicated = llm_job(&handle, llm);
    for offset in 0..3 {
        observe(&handle, duplicated, micros + offset, response(200));
    }
    let rejected = |status, message: &str| failure(provider_failure(message, Some(status), false));
    for observation in [
        rejected(429, "rate limited"),
        rejected(401, "bad key"),
        rejected(400, "bad body"),
        rejected(503, "overloaded"),
        failure(ProviderFailure {
            message: "connection reset".into(),
            ..ProviderFailure::default()
        }),
        AvailabilityObservation {
            status_code: Some(200),
            finish_reason: Some(ChatFinishReason::Stop),
            raw_finish_reason: Some("stop".into()),
            failure: Some(ProviderFailure {
                message: "stream reset".into(),
                ..ProviderFailure::default()
            }),
        },
    ] {
        let job = llm_job(&handle, llm);
        observe(&handle, job, micros, observation);
    }

    let report = consume_batch(&handle, usize::MAX);
    assert_eq!(report.consumed_records, 9);
    assert_eq!(report.samples_written, 7, "one sample per Job");
    let total = counts(&handle, provider, "model-a", micros);
    assert_eq!(
        total,
        AvailabilityCounts {
            available: 1,
            provider_server_error: 1,
            rate_limited: 1,
            auth_or_permission: 1,
            request_or_target: 1,
            unconfirmed: 2,
        },
        "each request counts once in its class"
    );
    worker.shutdown().expect("shutdown");
}

#[test]
fn the_first_record_fixes_the_minute_and_a_late_first_record_counts_in_its_older_minute() {
    let (_root, directory) = data_directory("aggregation-minutes");
    let worker = DatabaseWorker::start(&directory).expect("start database");
    let handle = worker.handle();
    let (provider, llm) = llm_setup(&handle, "model-a");
    let job = llm_job(&handle, llm);
    observe(&handle, job, at_minute(5), response(200));
    consume_batch(&handle, 10);

    // A backdated later record moves no bucket: the sample stays in minute 5.
    observe(
        &handle,
        job,
        at_minute(4),
        failure(provider_failure("overloaded", Some(503), true)),
    );
    consume_batch(&handle, 10);
    assert_eq!(
        counts(&handle, provider, "model-a", at_minute(5)),
        AvailabilityCounts {
            unconfirmed: 1,
            ..AvailabilityCounts::default()
        },
        "the sample is counted once, in its first record's minute"
    );
    assert_eq!(
        bucket(&handle, provider, "model-a", at_minute(4)),
        None,
        "no empty minute is created"
    );

    // A Job whose first record was recorded after a later minute's records counts in its own older minute.
    let late = llm_job(&handle, llm);
    observe(&handle, late, at_minute(3), response(200));
    consume_batch(&handle, 10);
    assert_eq!(
        counts(&handle, provider, "model-a", at_minute(3)).available,
        1
    );
    assert_eq!(
        counts(&handle, provider, "model-a", at_minute(5)).unconfirmed,
        1,
        "the later minute is untouched"
    );
    worker.shutdown().expect("shutdown");
}

#[test]
fn grouping_follows_the_frozen_provider_and_exact_model_across_profile_edits() {
    let (_root, directory) = data_directory("aggregation-grouping");
    let worker = DatabaseWorker::start(&directory).expect("start database");
    let handle = worker.handle();
    let (first, a) = llm_setup(&handle, "model-a");
    let b = llm_profile(
        &handle,
        first,
        llm_config("model-a", &[ModelCapability::Chat]),
    );
    let other = provider(&handle, "other");
    let c = llm_profile(
        &handle,
        other,
        llm_config("model-a", &[ModelCapability::Chat]),
    );
    let d = llm_profile(
        &handle,
        first,
        llm_config("Model-A", &[ModelCapability::Chat]),
    );
    let micros = at_minute(7);

    let before_edit = llm_job(&handle, a);
    observe(&handle, before_edit, micros, response(200));
    wait(handle.start_job(before_edit)).expect("start job");
    wait(handle.finalize_job(before_edit, JobFinalization::Completed(result("done"))))
        .expect("finish job");
    wait(handle.update_profile(
        a,
        NewProfile {
            name: "a".into(),
            config: NewProfileConfig::Llm {
                provider: first,
                config: llm_config("model-b", &[ModelCapability::Chat]),
            },
        },
    ))
    .expect("edit the profile");
    let after_edit = llm_job(&handle, a);
    observe(&handle, after_edit, micros, response(200));
    for llm in [b, c, d] {
        let job = llm_job(&handle, llm);
        observe(&handle, job, micros, response(200));
    }
    drain(&handle);

    assert_eq!(
        counts(&handle, first, "model-a", micros).available,
        2,
        "the edited Profile's earlier Job keeps its frozen model, and a second Profile shares the pair"
    );
    assert_eq!(
        counts(&handle, first, "model-b", micros).available,
        1,
        "the Job after the edit uses the new model"
    );
    assert_eq!(
        counts(&handle, other, "model-a", micros).available,
        1,
        "the same model at another Provider is another bucket"
    );
    assert_eq!(
        counts(&handle, first, "Model-A", micros).available,
        1,
        "model names are compared exactly"
    );
    worker.shutdown().expect("shutdown");
}

#[test]
fn a_retry_is_a_separate_request_in_the_counts() {
    let (_root, directory) = data_directory("aggregation-retry");
    let worker = DatabaseWorker::start(&directory).expect("start database");
    let handle = worker.handle();
    let Configured {
        provider,
        targeted,
        agent,
        ..
    } = configured(&handle);
    let micros = at_minute(9);
    let original = configured_child(&handle, agent, targeted);
    observe(
        &handle,
        original,
        micros,
        failure(provider_failure("overloaded", Some(503), true)),
    );
    wait(handle.finalize_job(
        original,
        JobFinalization::Failed(result("first try failed")),
    ))
    .expect("finish");
    let input = wait(handle.job(original))
        .expect("read job")
        .expect("job exists")
        .input;
    let retry = wait(handle.retry_job(JobRetry {
        original,
        input: RetryInput::Unchanged,
        name: None,
        execution_mode: ExecutionMode::Once,
        eligible_at: None,
        scheduled_at: None,
        next_due_at: None,
        next_iteration_no: None,
        iteration_no: None,
        projects: Vec::new(),
        dependencies: Vec::new(),
        configuration: configuration(agent, targeted),
        assigned_executor: None,
        acknowledge_repeated_effects: false,
    }))
    .expect("retry with the same configuration");
    observe(&handle, retry, micros + 1, response(200));
    drain(&handle);

    let minute = counts(&handle, provider, "model-a", micros);
    assert_eq!(
        (
            minute.provider_server_error,
            minute.available,
            requests(minute)
        ),
        (1, 1, 2),
        "the failed attempt and its retry are two requests"
    );
    assert_eq!(
        input,
        wait(handle.job(retry))
            .expect("read job")
            .expect("job exists")
            .input
    );
    worker.shutdown().expect("shutdown");
}

#[test]
fn reopening_keeps_counts_and_progress_and_consumes_only_new_records() {
    let (_root, directory) = data_directory("aggregation-reopen");
    let worker = DatabaseWorker::start(&directory).expect("start database");
    let handle = worker.handle();
    let (provider, llm) = llm_setup(&handle, "model-a");
    let micros = at_minute(11);
    let completed = llm_job(&handle, llm);
    observe(&handle, completed, micros, response(200));
    let unconfirmed = llm_job(&handle, llm);
    observe(&handle, unconfirmed, micros, bare(200));
    assert_eq!(drain(&handle), 2);
    let before = (
        counts(&handle, provider, "model-a", micros),
        wait(handle.availability_progress()).expect("read progress"),
    );
    worker.shutdown().expect("shutdown");

    let worker = DatabaseWorker::start(&directory).expect("reopen database");
    let handle = worker.handle();
    assert_eq!(
        (
            counts(&handle, provider, "model-a", micros),
            wait(handle.availability_progress()).expect("read progress")
        ),
        before,
        "counts and progress survive reopening"
    );
    let later = llm_job(&handle, llm);
    observe(&handle, later, micros, response(200));
    let report = consume_batch(&handle, 10);
    assert_eq!(
        report.consumed_records, 1,
        "only the record recorded after the reopen is consumed"
    );
    let total = counts(&handle, provider, "model-a", micros);
    assert_eq!((total.available, total.unconfirmed), (2, 1));
    worker.shutdown().expect("shutdown");
}

#[test]
fn a_stale_preparation_applies_nothing_and_its_records_still_count_once() {
    let (_root, directory) = data_directory("aggregation-stale");
    let worker = DatabaseWorker::start(&directory).expect("start database");
    let handle = worker.handle();
    let (provider, llm) = llm_setup(&handle, "model-a");
    let micros = at_minute(13);
    for _ in 0..2 {
        let job = llm_job(&handle, llm);
        observe(&handle, job, micros, response(200));
    }
    // Both preparations read the same snapshot, before either commits.
    let first = wait(handle.prepare_availability(1)).expect("prepare the first record");
    let second = wait(handle.prepare_availability(2)).expect("prepare both records");
    assert!(
        wait(handle.commit_availability(first))
            .expect("commit")
            .is_some()
    );
    let after_first = (
        counts(&handle, provider, "model-a", micros),
        wait(handle.availability_progress()).expect("read progress"),
    );
    assert_eq!(
        wait(handle.commit_availability(second)).expect("commit the stale preparation"),
        None,
        "a preparation overtaken by a commit is refused"
    );
    assert_eq!(
        (
            counts(&handle, provider, "model-a", micros),
            wait(handle.availability_progress()).expect("read progress")
        ),
        after_first,
        "a refused preparation changes no count and no progress"
    );
    assert_eq!(
        drain(&handle),
        1,
        "the record the stale batch named is consumed once"
    );
    assert_eq!(requests(counts(&handle, provider, "model-a", micros)), 2);
    worker.shutdown().expect("shutdown");
}

#[test]
fn a_failed_commit_leaves_samples_counts_and_progress_unchanged() {
    let (_root, directory) = data_directory("aggregation-failed");
    let worker = DatabaseWorker::start(&directory).expect("start database");
    let handle = worker.handle();
    let (provider, llm) = llm_setup(&handle, "model-a");
    let micros = at_minute(15);
    let job = llm_job(&handle, llm);
    observe(&handle, job, micros, response(200));
    observe(&handle, job, micros + 1, bare(200));

    let mut plan = wait(handle.prepare_availability(10)).expect("prepare");
    plan.inject_failure = true;
    let error = wait(handle.commit_availability(plan))
        .expect_err("the injected failure rolls the commit back");
    assert!(
        matches!(error, DatabaseError::RolledBack(_)),
        "got {error:?}"
    );
    // A storage failure latches the database, as every storage failure does:
    // reads are refused until it is reopened.
    assert!(
        matches!(
            wait(handle.availability_progress()),
            Err(DatabaseError::ReadsStorageFailed(_))
        ),
        "the latched database refuses reads"
    );
    assert!(
        matches!(worker.shutdown(), Err(DatabaseError::StorageUnavailable(_))),
        "shutdown reports the storage failure that latched the database"
    );

    let worker = DatabaseWorker::start(&directory).expect("reopen database");
    let handle = worker.handle();
    assert_eq!(
        wait(handle.availability_progress()).expect("read progress"),
        AvailabilityAggregationProgress::default()
    );
    assert_eq!(bucket(&handle, provider, "model-a", micros), None);
    assert!(
        wait(handle.request_sample(RequestRef {
            job,
            request: RequestNumber::FIRST
        }))
        .expect("read sample")
        .value
        .is_none()
    );
    assert_eq!(drain(&handle), 2, "the records are still unconsumed");
    assert_eq!(
        counts(&handle, provider, "model-a", micros).available,
        1,
        "the request counts once after the retry"
    );
    worker.shutdown().expect("shutdown");
}

#[test]
fn consumption_reads_no_project_associations() {
    let (_root, directory) = data_directory("aggregation-projects");
    let worker = DatabaseWorker::start(&directory).expect("start database");
    let handle = worker.handle();
    let (provider, llm) = llm_setup(&handle, "model-a");
    let project = project(&handle, "project");
    let job = create_job(
        &handle,
        NewJob {
            configuration: llm_only(llm),
            ..new_job("request", handle.genesis(), vec![project])
        },
    );
    observe(&handle, job, at_minute(17), response(200));
    wait(handle.with_database(crate::storage::break_project_links)).expect("arm the probe");
    assert!(
        matches!(wait(handle.job(job)), Err(DatabaseError::Read(_))),
        "the probe is armed"
    );

    assert_eq!(consume_batch(&handle, 10).consumed_records, 1);
    assert_eq!(
        counts(&handle, provider, "model-a", at_minute(17)).available,
        1
    );
    worker.shutdown().expect("shutdown");
}

/// Every operation marker, in order.
fn markers(handle: &DatabaseHandle) -> Vec<crate::persistence::OperationMarker> {
    wait(handle.operation_markers(None, 1_000)).expect("read markers")
}

/// Commits the next batch through a plan that counts the writes it performs.
/// Returns the report the plan gave before it committed and the sample
/// inserts, bucket inserts and bucket removals the commit made.
fn commit_probed(handle: &DatabaseHandle, limit: usize) -> (AvailabilityBatchReport, [usize; 3]) {
    use std::sync::atomic::Ordering::SeqCst;
    let mut plan = wait(handle.prepare_availability(limit)).expect("prepare");
    let probe = std::sync::Arc::new(crate::storage::WriteProbe::default());
    plan.probe = Some(probe.clone());
    let report = plan.report(1).expect("the plan consumes records");
    assert!(
        wait(handle.commit_availability(plan))
            .expect("commit")
            .is_some()
    );
    let writes = [
        probe.sample_inserts.load(SeqCst),
        probe.bucket_inserts.load(SeqCst),
        probe.bucket_removes.load(SeqCst),
    ];
    (report, writes)
}

#[test]
fn an_empty_prepared_plan_commits_nothing_with_or_without_earlier_progress() {
    let (_root, directory) = data_directory("aggregation-empty-plan");
    let worker = DatabaseWorker::start(&directory).expect("start database");
    let handle = worker.handle();
    let (provider, llm) = llm_setup(&handle, "model-a");
    let micros = at_minute(21);

    let empty = wait(handle.prepare_availability(10)).expect("prepare");
    assert_eq!(empty.consumed_through(), None);
    let before = markers(&handle);
    assert_eq!(
        wait(handle.commit_availability(empty)).expect("commit an empty plan"),
        None
    );
    assert_eq!(
        wait(handle.availability_progress()).expect("read progress"),
        AvailabilityAggregationProgress::default()
    );
    assert_eq!(markers(&handle), before, "an empty plan makes no marker");

    let job = llm_job(&handle, llm);
    observe(&handle, job, micros, response(200));
    consume_batch(&handle, 10);
    let progress = wait(handle.availability_progress()).expect("read progress");
    let before = markers(&handle);
    let empty = wait(handle.prepare_availability(10)).expect("prepare");
    assert_eq!(
        empty.consumed_through(),
        progress.consumed_through,
        "the empty plan still names the earlier progress"
    );
    assert_eq!(
        wait(handle.commit_availability(empty)).expect("commit an empty plan"),
        None
    );
    assert_eq!(
        wait(handle.availability_progress()).expect("read progress"),
        progress,
        "progress and its update time are unchanged"
    );
    assert_eq!(markers(&handle), before, "an empty plan makes no marker");
    assert_eq!(
        counts(&handle, provider, "model-a", micros).available,
        1,
        "the counts are unchanged"
    );
    worker.shutdown().expect("shutdown");
}

#[test]
fn unchanged_derived_rows_are_not_rewritten_but_progress_still_advances() {
    let (_root, directory) = data_directory("aggregation-unchanged-rows");
    let worker = DatabaseWorker::start(&directory).expect("start database");
    let handle = worker.handle();
    let (provider, llm) = llm_setup(&handle, "model-a");
    let micros = at_minute(23);

    // Duplicate evidence: the request stays Available in the same minute.
    let duplicated = llm_job(&handle, llm);
    observe(&handle, duplicated, micros, response(200));
    consume_batch(&handle, 10);
    let sample = wait(handle.request_sample(RequestRef {
        job: duplicated,
        request: RequestNumber::FIRST,
    }))
    .expect("read sample");
    observe(&handle, duplicated, micros + 1, response(200));
    let markers_before = markers(&handle).len();
    let (report, writes) = commit_probed(&handle, 10);
    assert_eq!(
        (
            report.consumed_records,
            report.samples_written,
            report.buckets_written
        ),
        (1, 0, 0),
        "the report counts only rows that change"
    );
    assert_eq!(writes, [0, 0, 0], "no derived row is rewritten");
    let progress = wait(handle.availability_progress()).expect("read progress");
    assert_eq!(
        progress.consumed_through,
        Some(report.consumed_through),
        "progress advances past the real record"
    );
    assert_eq!(
        markers(&handle).len(),
        markers_before + 1,
        "with its marker"
    );
    assert_eq!(
        wait(handle.request_sample(RequestRef {
            job: duplicated,
            request: RequestNumber::FIRST
        }))
        .expect("read sample")
        .value,
        sample.value
    );
    assert_eq!(requests(counts(&handle, provider, "model-a", micros)), 1);

    // A batch of one unchanged and one new request writes only the new one.
    let fresh = llm_job(&handle, llm);
    observe(&handle, duplicated, micros + 2, response(200));
    observe(&handle, fresh, micros + 3, response(200));
    let (report, writes) = commit_probed(&handle, 10);
    assert_eq!(
        (report.samples_written, report.buckets_written),
        (1, 1),
        "the new request's sample and its bucket"
    );
    assert_eq!(writes, [1, 1, 0]);
    assert_eq!(counts(&handle, provider, "model-a", micros).available, 2);

    // A changed sample whose class and minute stay the same leaves its bucket.
    let contradicted = llm_job(&handle, llm);
    observe(&handle, contradicted, micros + 4, bare(200));
    consume_batch(&handle, 10);
    observe(
        &handle,
        contradicted,
        micros + 5,
        failure(provider_failure("overloaded", Some(503), true)),
    );
    let (report, writes) = commit_probed(&handle, 10);
    assert_eq!(
        (report.samples_written, report.buckets_written),
        (1, 0),
        "the contradiction changes the sample row but not its bucket"
    );
    assert_eq!(writes, [1, 0, 0]);
    assert_eq!(counts(&handle, provider, "model-a", micros).unconfirmed, 1);
    worker.shutdown().expect("shutdown");
}

#[test]
fn counts_that_net_to_the_same_bytes_leave_their_bucket_alone() {
    let (_root, directory) = data_directory("aggregation-net-unchanged");
    let worker = DatabaseWorker::start(&directory).expect("start database");
    let handle = worker.handle();
    let (provider, llm) = llm_setup(&handle, "model-a");
    let micros = at_minute(25);
    let (leaving, arriving) = (llm_job(&handle, llm), llm_job(&handle, llm));
    observe(&handle, leaving, micros, response(200));
    observe(&handle, arriving, micros + 1, bare(200));
    consume_batch(&handle, 10);
    let before = counts(&handle, provider, "model-a", micros);
    assert_eq!((before.available, before.unconfirmed), (1, 1));

    // One request leaves Available and one arrives, within one bucket.
    observe(
        &handle,
        leaving,
        micros + 2,
        failure(provider_failure("overloaded", Some(503), true)),
    );
    observe(
        &handle,
        arriving,
        micros + 3,
        AvailabilityObservation {
            status_code: None,
            finish_reason: Some(ChatFinishReason::Stop),
            raw_finish_reason: Some("stop".into()),
            failure: None,
        },
    );
    let (report, writes) = commit_probed(&handle, 10);
    assert_eq!(
        (report.samples_written, report.buckets_written),
        (2, 0),
        "two samples change and their bucket nets to the same bytes"
    );
    assert_eq!(writes, [2, 0, 0]);
    assert_eq!(counts(&handle, provider, "model-a", micros), before);
    let class = |job| {
        wait(handle.request_sample(RequestRef {
            job,
            request: RequestNumber::FIRST,
        }))
        .expect("read sample")
        .value
        .expect("sample exists")
        .sample
        .classification
    };
    assert_eq!(class(leaving), AvailabilityClassification::Unconfirmed);
    assert_eq!(class(arriving), AvailabilityClassification::Available);
    worker.shutdown().expect("shutdown");
}

#[test]
fn an_unchanged_bucket_still_guards_the_commit() {
    let (_root, directory) = data_directory("aggregation-unchanged-guard");
    let worker = DatabaseWorker::start(&directory).expect("start database");
    let handle = worker.handle();
    let (provider, llm) = llm_setup(&handle, "model-a");
    let micros = at_minute(27);
    let (leaving, arriving) = (llm_job(&handle, llm), llm_job(&handle, llm));
    observe(&handle, leaving, micros, response(200));
    observe(&handle, arriving, micros + 1, bare(200));
    consume_batch(&handle, 10);
    observe(
        &handle,
        leaving,
        micros + 2,
        failure(provider_failure("overloaded", Some(503), true)),
    );
    observe(
        &handle,
        arriving,
        micros + 3,
        AvailabilityObservation {
            status_code: None,
            finish_reason: Some(ChatFinishReason::Stop),
            raw_finish_reason: Some("stop".into()),
            failure: None,
        },
    );
    let plan = wait(handle.prepare_availability(10)).expect("prepare");
    assert_eq!(
        plan.buckets_written(),
        0,
        "the bucket is guarded, not written"
    );

    // Something else changes the guarded bucket without moving the progress.
    let moved = AvailabilityCounts {
        rate_limited: 7,
        ..AvailabilityCounts::default()
    };
    let minute = minute_of(at(micros));
    wait(handle.with_database(move |database| {
        crate::storage::replace_minute(database, provider, "model-a", minute, &moved)
    }))
    .expect("change the bucket");
    let progress = wait(handle.availability_progress()).expect("read progress");
    assert_eq!(
        wait(handle.commit_availability(plan)).expect("commit"),
        None,
        "a changed unchanged-bucket makes the preparation stale"
    );
    assert_eq!(
        wait(handle.availability_progress()).expect("read progress"),
        progress
    );
    assert_eq!(counts(&handle, provider, "model-a", micros), moved);
    worker.shutdown().expect("shutdown");
}

#[test]
fn the_batch_maximum_splits_one_requests_records_and_counts_it_once() {
    const MAXIMUM: usize = 1_000;
    let (_root, directory) = data_directory("aggregation-maximum");
    let worker = DatabaseWorker::start(&directory).expect("start database");
    let handle = worker.handle();
    let (provider, llm) = llm_setup(&handle, "model-a");
    let micros = at_minute(29);
    let job = llm_job(&handle, llm);

    // The queued records commit in groups; the last one completes the request.
    let total = MAXIMUM + 1;
    let mut recorded = 0;
    while recorded < total {
        let chunk = (total - recorded).min(200);
        let pending: Vec<_> = (recorded..recorded + chunk)
            .map(|index| {
                let observation = if index + 1 == total {
                    response(200)
                } else {
                    bare(200)
                };
                admit(handle.record_availability_stat(NewAvailabilityStat {
                    job,
                    request: RequestNumber::FIRST,
                    observed_at: at(micros + index as i64),
                    observation,
                }))
            })
            .collect();
        for wait_for in pending {
            wait_for().expect("record AvailabilityStat");
        }
        recorded += chunk;
    }

    let first = consume_batch(&handle, usize::MAX);
    assert_eq!(first.consumed_records, MAXIMUM, "the batch maximum");
    assert_eq!((first.samples_written, first.buckets_written), (1, 1));
    let partial = counts(&handle, provider, "model-a", micros);
    assert_eq!((partial.unconfirmed, requests(partial)), (1, 1));

    let rest = consume_batch(&handle, usize::MAX);
    assert_eq!(rest.consumed_records, 1, "the remainder");
    assert_eq!((rest.samples_written, rest.buckets_written), (1, 1));
    let whole = counts(&handle, provider, "model-a", micros);
    assert_eq!(
        (whole.available, whole.unconfirmed, requests(whole)),
        (1, 0, 1),
        "the request is counted once, in its final class"
    );
    assert_eq!(
        wait(handle.consume_availability(usize::MAX)).expect("consume"),
        AvailabilityBatch::Idle {
            consumed_through: Some(rest.consumed_through)
        }
    );
    worker.shutdown().expect("shutdown");
}

/// Two plans from one snapshot: committing the first makes the second stale.
/// Returns the stale plan.
fn stale_plan(handle: &DatabaseHandle) -> crate::storage::AvailabilityPlan {
    let first = wait(handle.prepare_availability(1)).expect("prepare");
    let stale = wait(handle.prepare_availability(1)).expect("prepare");
    assert!(
        wait(handle.commit_availability(first))
            .expect("commit")
            .is_some()
    );
    stale
}

/// A worker whose recovery does not depend on the free space of the machine.
fn grouping_worker(directory: &DataDirectory) -> DatabaseWorker {
    DatabaseWorker::start_with_config(
        directory,
        crate::disk_protection::FilesystemConfig {
            disk_lock_threshold_bytes: 1,
            disk_resume_threshold_bytes: 2,
            ..crate::disk_protection::FilesystemConfig::default()
        },
    )
    .expect("start database")
}

fn one_record(job: JobId, micros: i64) -> NewAvailabilityStat {
    NewAvailabilityStat {
        job,
        request: RequestNumber::FIRST,
        observed_at: at(micros),
        observation: response(200),
    }
}

#[test]
fn a_group_of_declines_makes_no_durable_commit() {
    use std::sync::atomic::Ordering::SeqCst;
    let (_root, directory) = data_directory("aggregation-declined-group");
    let worker = grouping_worker(&directory);
    let handle = worker.handle();
    let (_provider, llm) = llm_setup(&handle, "model-a");
    let micros = at_minute(31);
    let (job, drained) = (llm_job(&handle, llm), llm_job(&handle, llm));
    observe(&handle, drained, micros, response(200));
    consume_batch(&handle, 10);
    let empty = wait(handle.prepare_availability(10)).expect("prepare");
    assert!(empty.consumed_through().is_some());
    observe(&handle, job, micros + 1, response(200));
    observe(&handle, job, micros + 2, response(200));
    let stale = stale_plan(&handle);
    let progress = wait(handle.availability_progress()).expect("read progress");
    let before = markers(&handle);

    let probe = handle.group_probe();
    let (held, release) = probe.hold_next_group();
    let flushes = probe.flushes.load(SeqCst);
    let first = admit(handle.commit_availability(stale));
    held.recv_timeout(crate::test_support::WAIT)
        .expect("the worker holds the group's first command");
    let second = admit(handle.commit_availability(empty));
    release.send(()).expect("release the group");
    assert_eq!(first().expect("a stale commit declines"), None);
    assert_eq!(second().expect("an empty commit declines"), None);

    assert_eq!(
        probe.flushes.load(SeqCst),
        flushes,
        "a group that wrote nothing makes no durable commit"
    );
    assert_eq!(
        wait(handle.availability_progress()).expect("read progress"),
        progress
    );
    assert_eq!(markers(&handle), before);
    worker.shutdown().expect("shutdown");
}

#[test]
fn a_decline_beside_writes_is_not_flushed_or_acknowledged_early() {
    use std::sync::atomic::Ordering::SeqCst;
    let (_root, directory) = data_directory("aggregation-mixed-group");
    let worker = grouping_worker(&directory);
    let handle = worker.handle();
    let (_provider, llm) = llm_setup(&handle, "model-a");
    let micros = at_minute(33);
    let (job, first_write, second_write) = (
        llm_job(&handle, llm),
        llm_job(&handle, llm),
        llm_job(&handle, llm),
    );
    observe(&handle, job, micros, response(200));
    observe(&handle, job, micros + 1, response(200));
    let stale = stale_plan(&handle);

    let probe = handle.group_probe();
    let (held, release) = probe.hold_next_group();
    let flushes = probe.flushes.load(SeqCst);
    let declined = admit(handle.commit_availability(stale));
    held.recv_timeout(crate::test_support::WAIT)
        .expect("the worker holds the group's first command");
    let first = admit(handle.record_availability_stat(one_record(first_write, micros + 2)));
    let second = admit(handle.record_availability_stat(one_record(second_write, micros + 3)));
    release.send(()).expect("release the group");
    assert_eq!(declined().expect("the stale commit declines"), None);
    first().expect("the first write is acknowledged");
    second().expect("the second write is acknowledged");
    assert_eq!(
        probe.flushes.load(SeqCst),
        flushes + 1,
        "the writes get one durable commit"
    );
    worker.shutdown().expect("shutdown");

    let worker = DatabaseWorker::start(&directory).expect("reopen database");
    let handle = worker.handle();
    for job in [first_write, second_write] {
        assert_eq!(
            stats(&handle, job).len(),
            1,
            "the acknowledged write is durable"
        );
    }
    worker.shutdown().expect("shutdown");
}

#[test]
fn a_lost_group_commit_does_not_make_a_decline_an_uncertain_commit() {
    use std::sync::atomic::Ordering::SeqCst;
    let (_root, directory) = data_directory("aggregation-lost-group");
    let worker = grouping_worker(&directory);
    let handle = worker.handle();
    let (_provider, llm) = llm_setup(&handle, "model-a");
    let micros = at_minute(35);
    let (job, first_write, second_write) = (
        llm_job(&handle, llm),
        llm_job(&handle, llm),
        llm_job(&handle, llm),
    );
    observe(&handle, job, micros, response(200));
    observe(&handle, job, micros + 1, response(200));
    let stale = stale_plan(&handle);

    let probe = handle.group_probe();
    let (held, release) = probe.hold_next_group();
    let flushes = probe.flushes.load(SeqCst);
    let declined = admit(handle.commit_availability(stale));
    held.recv_timeout(crate::test_support::WAIT)
        .expect("the worker holds the group's first command");
    let first = admit(handle.record_availability_stat(one_record(first_write, micros + 2)));
    let second = admit(handle.record_availability_stat(one_record(second_write, micros + 3)));
    probe.fail_next_flush.store(true, SeqCst);
    release.send(()).expect("release the group");

    let unknown = |result: Result<AvailabilityStatId, DatabaseError>| match result {
        Err(DatabaseError::OutcomeUnknown { operation, .. }) => operation,
        other => panic!("a write in a lost group is unknown, got {other:?}"),
    };
    let mut written = vec![unknown(first()), unknown(second())];
    assert!(
        matches!(declined(), Err(DatabaseError::RolledBack(_))),
        "the declined command committed nothing, so it is rolled back and not unknown"
    );
    assert_eq!(probe.flushes.load(SeqCst), flushes + 1);

    let report = wait(handle.recover_storage()).expect("recover storage");
    let mut reconciled: Vec<_> = report
        .reconciled
        .iter()
        .map(|(operation, _)| *operation)
        .collect();
    written.sort();
    reconciled.sort();
    assert_eq!(
        reconciled, written,
        "only the writes of the lost group are reconciled"
    );
    worker.shutdown().expect("shutdown");
}

#[test]
fn a_retry_shares_a_frozen_input_only_with_the_same_configuration() {
    let (_root, directory) = data_directory("configured-retry");
    let worker = DatabaseWorker::start(&directory).expect("start database");
    let handle = worker.handle();
    let Configured {
        provider,
        targeted,
        agent,
        ..
    } = configured(&handle);
    let original = configured_child(&handle, agent, targeted);
    let input = wait(handle.job(original))
        .expect("read job")
        .expect("job exists")
        .input;
    wait(handle.finalize_job(
        original,
        JobFinalization::Failed(result("first try failed")),
    ))
    .expect("finish");
    let retry = |original, input, configuration| JobRetry {
        original,
        input,
        name: None,
        execution_mode: ExecutionMode::Once,
        eligible_at: None,
        scheduled_at: None,
        next_due_at: None,
        next_iteration_no: None,
        iteration_no: None,
        projects: Vec::new(),
        dependencies: Vec::new(),
        configuration,
        assigned_executor: None,
        acknowledge_repeated_effects: false,
    };
    let same = wait(handle.retry_job(retry(
        original,
        RetryInput::Unchanged,
        configuration(agent, targeted),
    )))
    .expect("retry with the same configuration");
    let cancel = |job| {
        wait(handle.finalize_job(job, JobFinalization::Cancelled(result("stopped"))))
            .expect("finish");
    };
    cancel(same);
    let of_retry = wait(handle.retry_job(retry(
        same,
        RetryInput::Unchanged,
        configuration(agent, targeted),
    )))
    .expect("retry the retry");
    assert_eq!(
        wait(handle.input_jobs(input)).expect("read input Jobs"),
        [original, same, of_retry]
    );
    assert_eq!(
        violation(wait(handle.retry_job(retry(
            original,
            RetryInput::Unchanged,
            None
        )))),
        Violation::ConfigurationMismatch(input),
        "dropping the configuration changes it"
    );
    assert_eq!(
        violation(wait(handle.retry_job(retry(
            original,
            RetryInput::Unchanged,
            llm_only(targeted)
        )))),
        Violation::ConfigurationMismatch(input),
        "dropping only the Agent Profile changes it too"
    );
    cancel(of_retry);

    wait(handle.update_profile(targeted, llm_edit(provider, "model-b")))
        .expect("edit after every holder finished");
    let jobs_before = wait(handle.jobs(None, 100)).expect("read jobs");
    assert_eq!(
        violation(wait(handle.retry_job(retry(
            original,
            RetryInput::Unchanged,
            configuration(agent, targeted)
        )))),
        Violation::ConfigurationMismatch(input),
        "the same Profile IDs no longer mean the configuration the input froze"
    );
    assert_eq!(
        wait(handle.jobs(None, 100)).expect("read jobs"),
        jobs_before,
        "the refused retry created nothing"
    );
    let frozen = wait(handle.job_input(input))
        .expect("read input")
        .expect("input exists")
        .configuration
        .expect("frozen");
    assert_eq!(
        frozen.llm.config.model, "model-a",
        "the old input keeps its snapshot"
    );

    let new_input =
        wait(handle.save_job_input(None, JobInputDraft::default())).expect("save a new input");
    let changed = wait(handle.retry_job(retry(
        original,
        RetryInput::Changed(new_input),
        configuration(agent, targeted),
    )))
    .expect("retry with a new input");
    assert_eq!(frozen_model(&handle, changed), "model-b");
    assert_eq!(
        wait(handle.input_jobs(new_input)).expect("read input Jobs"),
        [changed]
    );
    assert_eq!(
        wait(handle.input_jobs(input)).expect("read input Jobs"),
        [original, same, of_retry]
    );
    assert_eq!(
        wait(handle.retries(original)).expect("read retries"),
        [same, changed]
    );
    worker.shutdown().expect("shutdown");
    assert_relationships_consistent(&directory);
}

// ---- Token annotations and capture ------------------------------------------------

use crate::database::TokenConsumeError;
use crate::openai_compatible::ChatConversation;
use crate::orchestration::execution_runtime::{finish_provider_job, output_sink};
use crate::persistence::{Measurements as TokenMeasurements, NewLlmProvider as TokenProvider};
use crate::provider_execution::scripted::{Step, payload};
use crate::provider_execution::{
    CancellationToken, CredentialResolver, ProviderExecutionConfig, ProviderFact, announce,
    execute_chat_with_sink, read_response,
};
use crate::token_usage::{
    RequestEligibility, TokenAggregationProgress, TokenAnnotation, TokenBatch, TokenBatchReport,
    TokenCount, TokenMinuteTotals, TokenRequestSample, TokenUsageState, UsageFinality,
};
use std::collections::BTreeMap;
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
    let plain = create_job(&handle, new_job("plain", handle.genesis(), Vec::new()));
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
    for annotation in [eligible, TokenAnnotation::excluded()] {
        assert_eq!(
            violation(wait(handle.record_metrics(token_metrics(
                plain,
                30,
                Some(annotation),
                [None, None]
            )))),
            Violation::NotAnLlmExecution(plain)
        );
    }
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
    let executor =
        executor(handle, "llm worker", ExecutorType::Llm, &[OPENAI]).expect("register Executor");
    let job = llm_job(handle, llm);
    wait(handle.start_job(job)).expect("start job");
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
    let provider = wait(handle.create_llm_provider(TokenProvider {
        name: "local".into(),
        transport: OPENAI,
        endpoint,
        credential_ref: None,
    }))
    .expect("create provider");
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
    let plain = create_job(&handle, new_job("plain", handle.genesis(), Vec::new()));
    wait(handle.start_job(plain)).expect("start job");
    let input = wait(handle.job(plain))
        .expect("read job")
        .expect("job exists")
        .input;
    let executor = executor(&handle, "llm worker", ExecutorType::Llm, &[OPENAI]).expect("register");
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
        Err(ExecutionRuntimeError::ObservationsNotRecorded { receipt, .. }) => {
            assert_eq!(receipt.job, plain);
        }
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

/// A started local Job that runs `llm` directly and may complete only once
/// `prerequisite` meets `condition`. A Job whose dependency is not satisfied
/// can no longer start, so it is started as a database written before that
/// check could hold it.
fn dependent_run(
    handle: &DatabaseHandle,
    llm: ProfileId,
    prerequisite: JobId,
    condition: DependencyCondition,
) -> TokenRun {
    let executor =
        executor(handle, "llm worker", ExecutorType::Llm, &[OPENAI]).expect("register Executor");
    let job = create_job(
        handle,
        NewJob {
            configuration: llm_only(llm),
            dependencies: vec![(prerequisite, condition)],
            ..new_job("dependent request", handle.genesis(), Vec::new())
        },
    );
    match wait(handle.start_local_job(job)) {
        Ok(()) => {}
        Err(DatabaseError::Rejected(Violation::UnsatisfiedDependency { .. })) => {
            wait(handle.start_preexisting_local_job(job)).expect("start as before the check")
        }
        Err(error) => panic!("start the Job locally: {error}"),
    }
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

/// A successful answer: text, running and final usage, a stop finish.
fn answered() -> Vec<Step> {
    vec![
        Step::Chunk(CUM_10),
        Step::Chunk(STOP_CHUNK),
        Step::Chunk(FINAL_INPUT_ONLY),
        Step::Chunk(DONE_CHUNK),
    ]
}

/// The final observations of one successful execution are recorded exactly
/// once, as the Provider answered.
fn assert_answer_recorded(handle: &DatabaseHandle, job: JobId, case: &str) {
    let availability =
        wait(handle.job_availability(job, None, 10)).expect("read AvailabilityStats");
    assert_eq!(
        availability.len(),
        1,
        "{case}: one availability observation"
    );
    let observation = &availability[0].observation;
    assert_eq!(
        (observation.status_code, observation.finish_reason),
        (Some(200), Some(ChatFinishReason::Stop)),
        "{case}"
    );
    assert!(
        observation.failure.is_none(),
        "{case}: no Provider failure is made up"
    );
    assert_eq!(
        observation.classification(),
        AvailabilityClassification::Available,
        "{case}"
    );
    let metrics = job_metrics(handle, job);
    let timing: Vec<_> = metrics
        .iter()
        .filter(|record| record.token.is_none())
        .collect();
    assert_eq!(timing.len(), 1, "{case}: one timing record");
    assert!(
        timing[0].measurements.ttfb_ms.is_some()
            && timing[0].measurements.request_finished_in_ms.is_some(),
        "{case}"
    );
    assert!(
        metrics
            .iter()
            .any(|record| record.measurements.usage.input_tokens == Some(40)),
        "{case}: the final usage is kept"
    );
}

/// The final result a refused completion settled Failed with: a local
/// reason, no reached goal, and the Provider's answer kept.
fn assert_failed_with_the_answer(handle: &DatabaseHandle, job: JobId, case: &str) {
    let record = wait(handle.job(job))
        .expect("read job")
        .expect("job exists");
    assert!(record.failed_at.is_some(), "{case}: the Job failed");
    assert_eq!(
        (record.completed_at, record.cancelled_at, record.unknown_at),
        (None, None, None),
        "{case}: no other outcome is claimed"
    );
    let outputs = wait(handle.job_outputs(job, None, 100)).expect("read outputs");
    let last = outputs.last().expect("a final result");
    assert_eq!(last.kind, OutputKind::Result, "{case}");
    assert_eq!(last.goal_reached, Some(false), "{case}");
    assert!(
        last.summary
            .as_deref()
            .is_some_and(|summary| summary.contains("cannot complete")),
        "{case}: {:?}",
        last.summary
    );
    assert_eq!(
        last.value,
        Some(json!({"text": "a", "finish_reason": "stop"})),
        "{case}: the Provider's answer is kept"
    );
}

#[test]
fn a_provider_success_whose_job_may_not_complete_settles_failed_with_its_evidence() {
    let (_root, directory) = data_directory("refused-completion-evidence");
    let worker = DatabaseWorker::start(&directory).expect("start database");
    let handle = worker.handle();
    let (_provider, llm) = llm_setup(&handle, "model-a");
    let settled = |name: &str, finalization: JobFinalization| {
        let job = create_job(&handle, new_job(name, handle.genesis(), Vec::new()));
        wait(handle.start_job(job)).expect("start");
        wait(handle.finalize_job(job, finalization)).expect("settle");
        job
    };
    let succeeded = settled("succeeded", JobFinalization::Completed(result("done")));
    let failed = settled("failed", JobFinalization::Failed(result("failed")));
    let waiting = create_job(&handle, new_job("waiting", handle.genesis(), Vec::new()));
    let cases = [
        (
            "a satisfied dependency",
            succeeded,
            DependencyCondition::Success,
            None,
        ),
        (
            "an unsatisfied Success dependency",
            failed,
            DependencyCondition::Success,
            Some(DependencyState::Impossible),
        ),
        (
            "a dependency still waiting",
            waiting,
            DependencyCondition::Settled,
            Some(DependencyState::Waiting),
        ),
    ];
    let mut jobs = Vec::new();
    for (case, prerequisite, condition, refusal) in cases {
        let run = dependent_run(&handle, llm, prerequisite, condition);
        let settlement = drive(&handle, run, answered(), BOUND);
        match (refusal, settlement) {
            (None, Ok(receipt)) => {
                assert_eq!(receipt.job, run.job);
                let job = wait(handle.job(run.job))
                    .expect("read job")
                    .expect("job exists");
                assert!(
                    job.completed_at.is_some() && job.failed_at.is_none(),
                    "{case}"
                );
            }
            (
                Some(expected),
                Err(ExecutionRuntimeError::CompletionRefused {
                    receipt,
                    source,
                    observations,
                }),
            ) => {
                assert_eq!(receipt.job, run.job, "{case}");
                assert!(receipt.result.is_some(), "{case}: a result was published");
                assert_eq!(
                    source,
                    Violation::UnsatisfiedDependency {
                        job: run.job,
                        prerequisite,
                        state: expected
                    },
                    "{case}"
                );
                assert!(
                    observations.is_none(),
                    "{case}: every observation was recorded"
                );
                assert_failed_with_the_answer(&handle, run.job, case);
            }
            (refusal, other) => panic!("{case}: expected the refusal {refusal:?}, got {other:?}"),
        }
        assert_answer_recorded(&handle, run.job, case);
        jobs.push(run.job);
    }
    assert!(
        blocking(&handle, llm).is_empty(),
        "every settled Job released the Profile"
    );
    worker.shutdown().expect("shutdown");

    let worker = DatabaseWorker::start(&directory).expect("restart");
    let handle = worker.handle();
    assert!(
        handle.abandoned_at_open().is_empty(),
        "every settlement released its local claim"
    );
    for job in jobs {
        assert_answer_recorded(&handle, job, "after a restart");
    }
    worker.shutdown().expect("shutdown");
}

#[test]
fn a_refused_completion_reports_the_observations_it_could_not_record() {
    let (_root, directory) = data_directory("refused-completion-gap");
    let worker = DatabaseWorker::start(&directory).expect("start database");
    let handle = worker.handle();
    let (_provider, llm) = llm_setup(&handle, "model-a");
    let failed = create_job(&handle, new_job("failed", handle.genesis(), Vec::new()));
    wait(handle.start_job(failed)).expect("start");
    wait(handle.finalize_job(failed, JobFinalization::Failed(result("failed")))).expect("fail");
    let run = dependent_run(&handle, llm, failed, DependencyCondition::Success);
    let missing_job = JobId::new(9_999_999);
    let settlement = {
        let handle = handle.clone();
        on_ntex(async move {
            let config = ProviderExecutionConfig::default();
            // Streamed observations go to a Job that does not exist, so each
            // of those writes is refused; no text is streamed to fail.
            let mut sink = output_sink(handle.clone(), missing_job);
            announce(Some(&mut sink), ProviderFact::Delivered).await;
            let execution = read_response(
                payload(
                    vec![
                        Step::Chunk(STOP_CHUNK),
                        Step::Chunk(FINAL_INPUT_ONLY),
                        Step::Chunk(DONE_CHUNK),
                    ],
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
    match settlement {
        Err(ExecutionRuntimeError::CompletionRefused {
            receipt,
            source,
            observations,
        }) => {
            assert_eq!(receipt.job, run.job);
            assert!(
                matches!(source, Violation::UnsatisfiedDependency { .. }),
                "{source:?}"
            );
            assert!(
                matches!(observations.as_deref(), Some(DatabaseError::Rejected(_))),
                "the unrecorded observation is reported: {observations:?}"
            );
        }
        other => panic!("expected the refused completion, got {other:?}"),
    }
    let job = wait(handle.job(run.job))
        .expect("read job")
        .expect("job exists");
    assert!(job.failed_at.is_some() && job.completed_at.is_none() && job.unknown_at.is_none());
    assert_eq!(
        wait(handle.job_availability(run.job, None, 10))
            .expect("read AvailabilityStats")
            .len(),
        1,
        "the observation that could be written is still recorded"
    );
    worker.shutdown().expect("shutdown");
}

// ---- Token aggregation ----------------------------------------------------------

type Counters5 = [Option<u32>; 5];
const NO_COUNTERS: Counters5 = [None; 5];
const ELIGIBLE: TokenAnnotation = TokenAnnotation::eligible(UsageFinality::Cumulative);
const ELIGIBLE_FINAL: TokenAnnotation = TokenAnnotation::eligible(UsageFinality::Final);

/// [input, output, cache read, cache write, reasoning] with only input and output.
fn io5(input: Option<u32>, output: Option<u32>) -> Counters5 {
    [input, output, None, None, None]
}

fn token_fact(
    handle: &DatabaseHandle,
    job: JobId,
    micros: i64,
    token: Option<TokenAnnotation>,
    counters: Counters5,
) -> crate::persistence::MetricsId {
    wait(handle.record_metrics(NewMetrics {
        job,
        request: RequestNumber::FIRST,
        observed_at: at(micros),
        measurements: TokenMeasurements {
            usage: NormalizedUsage {
                input_tokens: counters[0],
                output_tokens: counters[1],
                cache_read_tokens: counters[2],
                cache_write_tokens: counters[3],
                reasoning_tokens: counters[4],
                raw: None,
            },
            ..TokenMeasurements::default()
        },
        token,
    }))
    .expect("record Metrics")
}

/// Like [`token_fact`], but written as a database from before ingest
/// validation could hold it: even a fact that contradicts the Job's others.
fn preexisting_fact(
    handle: &DatabaseHandle,
    job: JobId,
    micros: i64,
    token: Option<TokenAnnotation>,
    counters: Counters5,
) -> crate::persistence::MetricsId {
    wait(handle.record_preexisting_metrics(NewMetrics {
        job,
        request: RequestNumber::FIRST,
        observed_at: at(micros),
        measurements: TokenMeasurements {
            usage: NormalizedUsage {
                input_tokens: counters[0],
                output_tokens: counters[1],
                cache_read_tokens: counters[2],
                cache_write_tokens: counters[3],
                reasoning_tokens: counters[4],
                raw: None,
            },
            ..TokenMeasurements::default()
        },
        token,
    }))
    .expect("record pre-existing Metrics")
}

fn consume_tokens(handle: &DatabaseHandle, limit: usize) -> TokenBatchReport {
    match wait(handle.consume_token_usage(limit)).expect("consume a Token batch") {
        TokenBatch::Consumed(report) => report,
        other => panic!("expected a committed Token batch, got {other:?}"),
    }
}

/// Consumes until the Token stream is idle; returns the records it took.
fn drain_tokens(handle: &DatabaseHandle) -> usize {
    let mut consumed = 0;
    loop {
        match wait(handle.consume_token_usage(usize::MAX)).expect("consume a Token batch") {
            TokenBatch::Consumed(report) => consumed += report.consumed_records,
            TokenBatch::Idle { .. } => return consumed,
            other => panic!("Token aggregation must not contend in a test, got {other:?}"),
        }
    }
}

fn token_progress(handle: &DatabaseHandle) -> TokenAggregationProgress {
    wait(handle.token_progress()).expect("read Token progress")
}

fn token_bucket(
    handle: &DatabaseHandle,
    provider: LlmProviderId,
    model: &str,
    micros: i64,
) -> Option<TokenMinuteTotals> {
    wait(handle.token_minutes(provider, model.to_owned(), minute_of(at(micros)), 1))
        .expect("read the Token minute")
        .value
        .pop()
        .map(|(_, totals)| totals)
}

fn token_sample(handle: &DatabaseHandle, job: JobId) -> Option<TokenRequestSample> {
    wait(handle.token_request(RequestRef {
        job,
        request: RequestNumber::FIRST,
    }))
    .expect("read the Token request")
    .value
    .and_then(|record| record.sample)
}

/// The stored state equals the pure reducer applied to each Job's records up to
/// the consumed prefix: per-request samples and every minute bucket.
fn assert_matches_reducer(handle: &DatabaseHandle, jobs: &[JobId]) {
    let through = token_progress(handle)
        .consumed_through
        .map_or(0, |id| id.get());
    let mut expected: BTreeMap<(LlmProviderId, String, i64), TokenMinuteTotals> = BTreeMap::new();
    for job in jobs {
        let records = job_metrics(handle, *job);
        let Some(target) = records.first().and_then(|record| record.target.clone()) else {
            continue;
        };
        let prefix: Vec<_> = records
            .into_iter()
            .filter(|record| record.id.get() <= through)
            .collect();
        let sample = TokenRequestSample::from_metrics(&prefix).expect("one Job");
        assert_eq!(
            token_sample(handle, *job),
            sample,
            "{job}: the stored request equals the reducer over the consumed prefix"
        );
        if let Some(sample) = sample {
            expected
                .entry((
                    target.frozen.provider.id,
                    target.frozen.config.model.clone(),
                    minute_of(sample.observed_at),
                ))
                .or_default()
                .step(&sample, true)
                .expect("the expected totals fit");
        }
    }
    for ((provider, model, minute), totals) in &expected {
        let stored = wait(handle.token_minutes(*provider, model.clone(), *minute, 1))
            .expect("read the Token minute")
            .value;
        assert_eq!(
            stored,
            [(*minute, *totals)],
            "{model} at minute {minute}: the bucket equals the reducer's totals"
        );
    }
}

fn token_markers(handle: &DatabaseHandle) -> Vec<crate::persistence::OperationMarker> {
    markers(handle)
        .into_iter()
        .filter(|marker| marker.kind == crate::persistence::OperationKind::ConsumeTokenUsage)
        .collect()
}

#[test]
fn an_empty_token_step_writes_nothing_and_a_skipped_batch_still_advances() {
    let (_root, directory) = data_directory("token-skipped");
    let worker = DatabaseWorker::start(&directory).expect("start database");
    let handle = worker.handle();
    let (provider, llm) = llm_setup(&handle, "model-a");
    let job = llm_job(&handle, llm);
    let plain = create_job(&handle, new_job("plain", handle.genesis(), Vec::new()));
    assert_eq!(
        wait(handle.consume_token_usage(10)).expect("consume"),
        TokenBatch::Idle {
            consumed_through: None
        }
    );
    assert_eq!(token_progress(&handle), TokenAggregationProgress::default());
    assert!(
        token_markers(&handle).is_empty(),
        "an idle step makes no marker"
    );

    // Generic, non-LLM and Excluded records are not request contributions.
    token_fact(&handle, job, at_minute(1), None, io5(Some(3), None));
    token_fact(&handle, plain, at_minute(1), None, io5(Some(4), None));
    let excluded = token_fact(
        &handle,
        job,
        at_minute(1),
        Some(TokenAnnotation::excluded()),
        NO_COUNTERS,
    );
    // The availability stream has its own progress.
    observe(&handle, job, at_minute(1), response(200));
    consume_batch(&handle, 10);
    let availability = wait(handle.availability_progress()).expect("read progress");
    assert_eq!(token_progress(&handle), TokenAggregationProgress::default());

    let report = consume_tokens(&handle, 10);
    assert_eq!(
        (
            report.consumed_records,
            report.unannotated_records,
            report.requests_written,
            report.buckets_written,
            report.consumed_through
        ),
        (3, 2, 1, 0, excluded),
        "every record counts, and only the Excluded one leaves evidence"
    );
    assert_eq!(token_progress(&handle).consumed_through, Some(excluded));
    assert_eq!(
        wait(handle.availability_progress()).expect("read progress"),
        availability,
        "the Token stream did not move the availability stream"
    );
    let batch_markers = token_markers(&handle);
    assert_eq!(
        batch_markers.len(),
        1,
        "a skipped batch still has its marker"
    );
    assert_eq!(batch_markers[0].subject, RecordRef::Metrics(excluded));
    assert_eq!(
        token_sample(&handle, job),
        None,
        "excluded work has no sample"
    );
    assert_eq!(
        wait(handle.token_request(RequestRef {
            job,
            request: RequestNumber::FIRST
        }))
        .expect("read")
        .value
        .map(|r| r.provider),
        Some(provider),
        "but its evidence and target are kept"
    );
    assert_eq!(
        token_bucket(&handle, provider, "model-a", at_minute(1)),
        None
    );

    let progress = token_progress(&handle);
    assert_eq!(
        wait(handle.consume_token_usage(10)).expect("consume"),
        TokenBatch::Idle {
            consumed_through: Some(excluded)
        }
    );
    assert_eq!(
        token_progress(&handle),
        progress,
        "an idle step writes no update time"
    );
    assert_eq!(token_markers(&handle).len(), 1);

    // The reverse independence: availability does not move Token progress.
    observe(&handle, job, at_minute(2), response(200));
    consume_batch(&handle, 10);
    assert_eq!(token_progress(&handle), progress);
    worker.shutdown().expect("shutdown");
}

#[test]
fn a_token_batch_takes_one_thousand_records_whatever_they_are() {
    let (_root, directory) = data_directory("token-boundary");
    let worker = DatabaseWorker::start(&directory).expect("start database");
    let handle = worker.handle();
    let (provider, llm) = llm_setup(&handle, "model-a");
    let job = llm_job(&handle, llm);
    let mut last = None;
    for chunk in 0..6 {
        let pending: Vec<_> = (0..200)
            .map(|offset| {
                let index = chunk * 200 + offset;
                // 1,000 generic records, then the one eligible anchor.
                let (token, counters) = if index < 1_000 {
                    (None, io5(Some(1), None))
                } else {
                    (Some(ELIGIBLE), NO_COUNTERS)
                };
                if index > 1_000 {
                    return None;
                }
                Some(admit(handle.record_metrics(NewMetrics {
                    job,
                    request: RequestNumber::FIRST,
                    observed_at: at(at_minute(3)),
                    measurements: TokenMeasurements {
                        usage: NormalizedUsage {
                            input_tokens: counters[0],
                            ..NormalizedUsage::default()
                        },
                        ..TokenMeasurements::default()
                    },
                    token,
                })))
            })
            .collect();
        for wait_for in pending.into_iter().flatten() {
            last = Some(wait_for().expect("record Metrics"));
        }
    }
    let last = last.expect("the eligible anchor");

    let first = consume_tokens(&handle, usize::MAX);
    assert_eq!(first.consumed_records, 1_000, "the batch maximum");
    assert_eq!(
        (
            first.unannotated_records,
            first.requests_written,
            first.buckets_written
        ),
        (1_000, 0, 0)
    );
    assert_eq!(
        token_bucket(&handle, provider, "model-a", at_minute(3)),
        None,
        "generic records anchor nothing"
    );
    let rest = consume_tokens(&handle, usize::MAX);
    assert_eq!((rest.consumed_records, rest.consumed_through), (1, last));
    let bucket = token_bucket(&handle, provider, "model-a", at_minute(3)).expect("a bucket");
    assert_eq!((bucket.requests, bucket.missing_requests), (1, 1));
    assert_eq!(
        wait(handle.consume_token_usage(usize::MAX)).expect("consume"),
        TokenBatch::Idle {
            consumed_through: Some(last)
        }
    );
    assert_matches_reducer(&handle, &[job]);
    worker.shutdown().expect("shutdown");
}

#[test]
fn a_request_split_across_batches_and_reopens_replaces_its_contribution() {
    let (_root, directory) = data_directory("token-split");
    let mut worker = DatabaseWorker::start(&directory).expect("start database");
    let mut handle = worker.handle();
    let (provider, llm) = llm_setup(&handle, "model-a");
    let job = llm_job(&handle, llm);
    let micros = at_minute(7);
    let reopen = |worker: DatabaseWorker| {
        worker.shutdown().expect("shutdown");
        let worker = DatabaseWorker::start(&directory).expect("reopen database");
        let handle = worker.handle();
        (worker, handle)
    };

    // Missing: an eligible anchor with no usage.
    token_fact(&handle, job, micros, Some(ELIGIBLE), NO_COUNTERS);
    consume_tokens(&handle, 1);
    let bucket = token_bucket(&handle, provider, "model-a", micros).expect("a bucket");
    assert_eq!(
        (
            bucket.requests,
            bucket.missing_requests,
            bucket.input.missing_requests,
            bucket.total.unknown_requests
        ),
        (1, 1, 1, 1)
    );
    assert_eq!(
        (bucket.input.provisional_sum, bucket.input.final_sum),
        (0, 0)
    );
    assert_matches_reducer(&handle, &[job]);

    // Provisional, after a reopen: the old contribution is replaced, not added to.
    (worker, handle) = reopen(worker);
    token_fact(
        &handle,
        job,
        micros + 1,
        Some(ELIGIBLE),
        io5(Some(25), Some(5)),
    );
    consume_tokens(&handle, 1);
    let bucket = token_bucket(&handle, provider, "model-a", micros).expect("a bucket");
    assert_eq!(
        (
            bucket.requests,
            bucket.missing_requests,
            bucket.incomplete_requests,
            bucket.input.provisional_requests,
            bucket.input.provisional_sum,
            bucket.output.provisional_sum,
            bucket.total.provisional_sum,
            bucket.total.unknown_requests,
        ),
        (1, 0, 1, 1, 25, 5, 30, 0)
    );
    assert_matches_reducer(&handle, &[job]);

    // A duplicate changes nothing physical, but the progress and marker advance.
    let markers_before = token_markers(&handle).len();
    token_fact(
        &handle,
        job,
        micros + 2,
        Some(ELIGIBLE),
        io5(Some(25), Some(5)),
    );
    let report = consume_tokens(&handle, 1);
    assert_eq!((report.requests_written, report.buckets_written), (0, 0));
    assert_eq!(token_markers(&handle).len(), markers_before + 1);
    assert_eq!(
        token_bucket(&handle, provider, "model-a", micros)
            .expect("a bucket")
            .input
            .provisional_sum,
        25,
        "a duplicate counts once"
    );

    // A final below the provisional maximum replaces it; output stays provisional.
    (worker, handle) = reopen(worker);
    token_fact(
        &handle,
        job,
        micros + 3,
        Some(ELIGIBLE_FINAL),
        io5(Some(10), None),
    );
    consume_tokens(&handle, 1);
    let bucket = token_bucket(&handle, provider, "model-a", micros).expect("a bucket");
    assert_eq!(
        (
            bucket.input.final_requests,
            bucket.input.final_sum,
            bucket.input.provisional_sum,
            bucket.output.provisional_sum,
            bucket.total.provisional_sum,
            bucket.complete_requests,
        ),
        (1, 10, 0, 5, 15, 0),
        "the final 10 wins over the provisional 25, and the total is still provisional"
    );
    assert_matches_reducer(&handle, &[job]);

    // Output becomes final in a separate record: Complete.
    token_fact(
        &handle,
        job,
        micros + 4,
        Some(ELIGIBLE_FINAL),
        io5(None, Some(7)),
    );
    consume_tokens(&handle, 1);
    let bucket = token_bucket(&handle, provider, "model-a", micros).expect("a bucket");
    assert_eq!(
        (
            bucket.complete_requests,
            bucket.incomplete_requests,
            bucket.total.final_requests,
            bucket.total.final_sum,
            bucket.total.provisional_sum,
            bucket.output.provisional_requests,
        ),
        (1, 0, 1, 17, 0, 0)
    );
    assert_matches_reducer(&handle, &[job]);

    // A conflicting final removes the output's known contribution, keeps input.
    (worker, handle) = reopen(worker);
    token_fact(
        &handle,
        job,
        micros + 5,
        Some(ELIGIBLE_FINAL),
        io5(None, Some(8)),
    );
    consume_tokens(&handle, 1);
    let bucket = token_bucket(&handle, provider, "model-a", micros).expect("a bucket");
    assert_eq!(
        (
            bucket.output.conflicted_requests,
            bucket.output.final_sum,
            bucket.output.final_requests,
            bucket.input.final_sum,
            bucket.total.unknown_requests,
            bucket.total.final_sum,
            bucket.incomplete_requests,
        ),
        (1, 0, 0, 10, 1, 0, 1),
        "the old known output and total are subtracted, not kept"
    );
    assert_matches_reducer(&handle, &[job]);
    assert_eq!(drain_tokens(&handle), 0);
    worker.shutdown().expect("shutdown");
}

#[test]
fn fields_zero_detail_and_totals_have_their_own_coverage_and_exact_sums() {
    let (_root, directory) = data_directory("token-values");
    let worker = DatabaseWorker::start(&directory).expect("start database");
    let handle = worker.handle();
    let (provider, llm) = llm_setup(&handle, "model-a");
    let micros = at_minute(9);
    let jobs: Vec<JobId> = (0..5).map(|_| llm_job(&handle, llm)).collect();
    // A: real zeros. B: the widest counts. C: input only. D: nothing reported.
    // E: valid core with conflicting optional detail.
    token_fact(
        &handle,
        jobs[0],
        micros,
        Some(ELIGIBLE_FINAL),
        [Some(0), Some(0), Some(0), None, None],
    );
    token_fact(
        &handle,
        jobs[1],
        micros,
        Some(ELIGIBLE_FINAL),
        io5(Some(u32::MAX), Some(u32::MAX)),
    );
    token_fact(
        &handle,
        jobs[2],
        micros,
        Some(ELIGIBLE_FINAL),
        io5(Some(5), None),
    );
    token_fact(&handle, jobs[3], micros, Some(ELIGIBLE), NO_COUNTERS);
    token_fact(
        &handle,
        jobs[4],
        micros,
        Some(ELIGIBLE_FINAL),
        [Some(2), Some(3), Some(1), None, None],
    );
    token_fact(
        &handle,
        jobs[4],
        micros + 1,
        Some(ELIGIBLE_FINAL),
        [None, None, Some(2), None, None],
    );
    assert_eq!(drain_tokens(&handle), 6);

    let bucket = token_bucket(&handle, provider, "model-a", micros).expect("a bucket");
    assert_eq!(
        (
            bucket.requests,
            bucket.complete_requests,
            bucket.incomplete_requests,
            bucket.missing_requests
        ),
        (5, 3, 1, 1),
        "optional detail never makes final core usage incomplete"
    );
    let wide = u128::from(u32::MAX);
    assert_eq!(
        (
            bucket.input.final_requests,
            bucket.input.final_sum,
            bucket.input.missing_requests
        ),
        (4, wide + 5 + 2, 1),
        "a reported zero is a known value; a missing one is coverage only"
    );
    assert_eq!(
        (
            bucket.output.final_requests,
            bucket.output.final_sum,
            bucket.output.missing_requests
        ),
        (3, wide + 3, 2)
    );
    assert_eq!(
        (
            bucket.total.final_requests,
            bucket.total.final_sum,
            bucket.total.unknown_requests
        ),
        (3, 2 * wide + 5, 2),
        "totals come from requests with both parts: C and D have none, and the sum passes u32::MAX"
    );
    assert_eq!(
        (
            bucket.cache_read.final_requests,
            bucket.cache_read.final_sum,
            bucket.cache_read.conflicted_requests,
            bucket.cache_read.missing_requests
        ),
        (1, 0, 1, 3)
    );
    assert_eq!(bucket.cache_write.missing_requests, 5);
    assert_matches_reducer(&handle, &jobs);

    // An all-missing cohort is not measured zero.
    let silent = llm_job(&handle, llm);
    token_fact(&handle, silent, at_minute(10), Some(ELIGIBLE), NO_COUNTERS);
    drain_tokens(&handle);
    let empty = token_bucket(&handle, provider, "model-a", at_minute(10)).expect("a bucket");
    assert_eq!((empty.requests, empty.input.missing_requests), (1, 1));
    assert_eq!(
        (
            empty.input.final_requests,
            empty.input.provisional_requests,
            empty.input.final_sum
        ),
        (0, 0, 0)
    );
    let zero = llm_job(&handle, llm);
    token_fact(
        &handle,
        zero,
        at_minute(11),
        Some(ELIGIBLE_FINAL),
        io5(Some(0), Some(0)),
    );
    drain_tokens(&handle);
    let measured = token_bucket(&handle, provider, "model-a", at_minute(11)).expect("a bucket");
    assert_eq!(
        (
            measured.input.final_requests,
            measured.input.missing_requests,
            measured.total.final_requests,
            measured.total.final_sum
        ),
        (1, 0, 1, 0),
        "a measured zero has coverage where an all-missing cohort has none"
    );
    worker.shutdown().expect("shutdown");
}

#[test]
fn the_sample_minute_is_the_first_eligible_record_and_no_empty_bucket_is_made() {
    let (_root, directory) = data_directory("token-time");
    let worker = DatabaseWorker::start(&directory).expect("start database");
    let handle = worker.handle();
    let (provider, llm) = llm_setup(&handle, "model-a");
    let job = llm_job(&handle, llm);
    // An earlier unannotated record anchors nothing; the lowest eligible ID does.
    token_fact(&handle, job, at_minute(1), None, io5(Some(1), None));
    token_fact(&handle, job, at_minute(5), Some(ELIGIBLE), NO_COUNTERS);
    assert_eq!(consume_tokens(&handle, 2).consumed_records, 2);
    // A later, backdated, higher-ID record does not move the bucket.
    token_fact(
        &handle,
        job,
        at_minute(2),
        Some(ELIGIBLE_FINAL),
        io5(Some(8), Some(9)),
    );
    token_fact(
        &handle,
        job,
        at_minute(8),
        Some(ELIGIBLE_FINAL),
        io5(Some(8), Some(9)),
    );
    assert_eq!(drain_tokens(&handle), 2);
    let bucket = token_bucket(&handle, provider, "model-a", at_minute(5)).expect("a bucket");
    assert_eq!((bucket.requests, bucket.complete_requests), (1, 1));
    for minute in [1, 2, 8] {
        assert_eq!(
            token_bucket(&handle, provider, "model-a", at_minute(minute)),
            None
        );
    }
    assert_eq!(
        token_sample(&handle, job).map(|sample| sample.observed_at),
        Some(at(at_minute(5)))
    );

    // Minute edges and negative time use the existing signed minute.
    let edge = [-60_000_001, -60_000_000, -1, 0, 59_999_999, 60_000_000];
    let jobs: Vec<JobId> = edge.iter().map(|_| llm_job(&handle, llm)).collect();
    for (job, micros) in jobs.iter().zip(edge) {
        token_fact(&handle, *job, micros, Some(ELIGIBLE), NO_COUNTERS);
    }
    drain_tokens(&handle);
    let window = wait(handle.token_minutes(provider, "model-a".into(), -3, 6))
        .expect("read the window")
        .value;
    assert_eq!(
        window
            .iter()
            .map(|(minute, totals)| (*minute, totals.requests))
            .collect::<Vec<_>>(),
        [(-2, 1), (-1, 2), (0, 2), (1, 1)],
        "each edge falls in its own signed minute and empty minutes have no row"
    );
    assert_matches_reducer(&handle, &jobs);
    worker.shutdown().expect("shutdown");
}

#[test]
fn targets_merge_by_provider_and_exact_model_and_retries_stay_separate() {
    let (_root, directory) = data_directory("token-targets");
    let worker = DatabaseWorker::start(&directory).expect("start database");
    let handle = worker.handle();
    let first = provider(&handle, "first");
    let second = provider(&handle, "second");
    let profile = |provider, model: &str| {
        llm_profile(
            &handle,
            provider,
            llm_config(model, &[ModelCapability::Chat]),
        )
    };
    let (a, b) = (profile(first, "m"), profile(first, "m"));
    let (other_provider, other_case) = (profile(second, "m"), profile(first, "M"));
    let micros = at_minute(12);
    let jobs: Vec<(JobId, LlmProviderId, &str)> = vec![
        (llm_job(&handle, a), first, "m"),
        (llm_job(&handle, b), first, "m"),
        (llm_job(&handle, other_provider), second, "m"),
        (llm_job(&handle, other_case), first, "M"),
    ];
    for (job, ..) in &jobs {
        token_fact(&handle, *job, micros, Some(ELIGIBLE), NO_COUNTERS);
    }
    drain_tokens(&handle);
    for (provider, model, expected) in [(first, "m", 2), (second, "m", 1), (first, "M", 1)] {
        assert_eq!(
            token_bucket(&handle, provider, model, micros).map(|totals| totals.requests),
            Some(expected),
            "{model} at {provider}"
        );
    }

    // An edited Profile keeps the historical target of its earlier Job.
    let edited = profile(first, "old");
    let before = llm_job(&handle, edited);
    wait(handle.start_job(before)).expect("start job");
    token_fact(&handle, before, micros, Some(ELIGIBLE), NO_COUNTERS);
    wait(handle.finalize_job(before, JobFinalization::Completed(result("done")))).expect("finish");
    wait(handle.update_profile(edited, llm_edit(first, "new"))).expect("edit the Profile");
    let after = llm_job(&handle, edited);
    token_fact(&handle, after, micros, Some(ELIGIBLE), NO_COUNTERS);
    drain_tokens(&handle);
    assert_eq!(
        token_bucket(&handle, first, "old", micros).map(|t| t.requests),
        Some(1)
    );
    assert_eq!(
        token_bucket(&handle, first, "new", micros).map(|t| t.requests),
        Some(1)
    );

    // A retry is a separate request even when the input is shared.
    let configured_set = configured(&handle);
    let original = configured_child(&handle, configured_set.agent, configured_set.targeted);
    let input = wait(handle.job(original))
        .expect("read job")
        .expect("job exists")
        .input;
    wait(handle.finalize_job(
        original,
        JobFinalization::Failed(result("first try failed")),
    ))
    .expect("finish");
    let retry = wait(handle.retry_job(JobRetry {
        original,
        input: RetryInput::Unchanged,
        name: None,
        execution_mode: ExecutionMode::Once,
        eligible_at: None,
        scheduled_at: None,
        next_due_at: None,
        next_iteration_no: None,
        iteration_no: None,
        projects: Vec::new(),
        dependencies: Vec::new(),
        configuration: configuration(configured_set.agent, configured_set.targeted),
        assigned_executor: None,
        acknowledge_repeated_effects: false,
    }))
    .expect("retry");
    assert_eq!(
        wait(handle.job(retry))
            .expect("read job")
            .expect("job exists")
            .input,
        input,
        "the retry shares the input"
    );
    token_fact(
        &handle,
        original,
        micros,
        Some(ELIGIBLE_FINAL),
        io5(Some(3), Some(4)),
    );
    token_fact(
        &handle,
        retry,
        micros,
        Some(ELIGIBLE_FINAL),
        io5(Some(3), Some(4)),
    );
    drain_tokens(&handle);
    let model = frozen_model(&handle, original);
    let bucket = token_bucket(&handle, configured_set.provider, &model, micros).expect("a bucket");
    assert_eq!(
        (bucket.requests, bucket.input.final_sum),
        (2, 6),
        "two requests, not one request counted twice"
    );
    let all: Vec<JobId> = jobs
        .iter()
        .map(|(job, ..)| *job)
        .chain([before, after, original, retry])
        .collect();
    assert_matches_reducer(&handle, &all);
    worker.shutdown().expect("shutdown");
}

#[test]
fn contradictory_eligibility_refuses_the_whole_batch_and_keeps_excluded_evidence() {
    let (_root, directory) = data_directory("token-contradiction");
    let worker = DatabaseWorker::start(&directory).expect("start database");
    let handle = worker.handle();
    let (provider, llm) = llm_setup(&handle, "model-a");
    let (bad, good) = (llm_job(&handle, llm), llm_job(&handle, llm));
    let micros = at_minute(14);
    let excluded = token_fact(
        &handle,
        bad,
        micros,
        Some(TokenAnnotation::excluded()),
        NO_COUNTERS,
    );
    // Batch one consumes only the Excluded claim: its evidence is stored.
    assert_eq!(consume_tokens(&handle, 1).consumed_through, excluded);
    let progress = token_progress(&handle);
    let state = wait(handle.token_request(RequestRef {
        job: bad,
        request: RequestNumber::FIRST,
    }))
    .expect("read")
    .value;
    assert!(state.is_some() && state.expect("state").sample.is_none());

    // A later eligible claim about the same request is a contradiction, and a
    // valid request in the same batch is not applied either.
    token_fact(&handle, good, micros, Some(ELIGIBLE), NO_COUNTERS);
    // Ingest refuses it now; a database from before that check may hold it.
    let eligible = preexisting_fact(&handle, bad, micros, Some(ELIGIBLE), io5(Some(1), Some(1)));
    let markers_before = token_markers(&handle).len();
    match wait(handle.consume_token_usage(10)) {
        Err(TokenConsumeError::Evidence {
            metrics,
            job,
            error,
        }) => {
            assert_eq!((metrics, job), (eligible, bad));
            assert_eq!(
                error,
                crate::token_usage::TokenEvidenceError::ContradictoryEligibility(RequestRef {
                    job: bad,
                    request: RequestNumber::FIRST
                })
            );
        }
        other => panic!("expected the contradiction to refuse the batch, got {other:?}"),
    }
    assert_eq!(token_progress(&handle), progress, "no progress");
    assert_eq!(token_markers(&handle).len(), markers_before, "no marker");
    assert_eq!(
        token_bucket(&handle, provider, "model-a", micros),
        None,
        "no counts"
    );
    assert_eq!(token_sample(&handle, good), None, "no partial state");
    assert_eq!(token_sample(&handle, bad), None);
    // The refusal repeats until the facts are dealt with; nothing guesses.
    assert!(matches!(
        wait(handle.consume_token_usage(10)),
        Err(TokenConsumeError::Evidence { .. })
    ));
    worker.shutdown().expect("shutdown");
}

fn token_plan(handle: &DatabaseHandle, limit: usize) -> crate::storage::TokenPlan {
    wait(handle.prepare_token(limit))
        .expect("prepare")
        .expect("no contradiction")
}

/// Commits the next Token batch through a plan that counts its writes.
fn commit_token_probed(handle: &DatabaseHandle, limit: usize) -> (TokenBatchReport, [usize; 3]) {
    use std::sync::atomic::Ordering::SeqCst;
    let mut plan = token_plan(handle, limit);
    let probe = std::sync::Arc::new(crate::storage::WriteProbe::default());
    plan.probe = Some(probe.clone());
    let report = plan.report(1).expect("the plan consumes records");
    assert!(wait(handle.commit_token(plan)).expect("commit").is_some());
    (
        report,
        [
            probe.sample_inserts.load(SeqCst),
            probe.bucket_inserts.load(SeqCst),
            probe.bucket_removes.load(SeqCst),
        ],
    )
}

#[test]
fn token_plans_commit_only_physical_changes_and_decline_when_stale() {
    let (_root, directory) = data_directory("token-plans");
    let worker = DatabaseWorker::start(&directory).expect("start database");
    let handle = worker.handle();
    let (provider, llm) = llm_setup(&handle, "model-a");
    let job = llm_job(&handle, llm);
    let micros = at_minute(16);

    // Empty plans commit nothing, before and after progress exists.
    let before = markers(&handle);
    let empty = token_plan(&handle, 10);
    assert_eq!(empty.consumed_through(), None);
    assert_eq!(wait(handle.commit_token(empty)).expect("commit"), None);
    token_fact(&handle, job, micros, Some(ELIGIBLE), io5(Some(4), Some(4)));
    consume_tokens(&handle, 10);
    let progress = token_progress(&handle);
    let marker_count = markers(&handle).len();
    let empty = token_plan(&handle, 10);
    assert_eq!(empty.consumed_through(), progress.consumed_through);
    assert_eq!(wait(handle.commit_token(empty)).expect("commit"), None);
    assert_eq!(token_progress(&handle), progress, "no new update time");
    assert_eq!(markers(&handle).len(), marker_count, "and no marker");
    assert!(before.len() < marker_count);

    // Duplicate evidence: no physical write, but progress and marker advance.
    token_fact(
        &handle,
        job,
        micros + 1,
        Some(ELIGIBLE),
        io5(Some(4), Some(4)),
    );
    let marker_count = markers(&handle).len();
    let (report, writes) = commit_token_probed(&handle, 10);
    assert_eq!((report.requests_written, report.buckets_written), (0, 0));
    assert_eq!(writes, [0, 0, 0], "reports and physical writes agree");
    assert_eq!(markers(&handle).len(), marker_count + 1);
    assert_ne!(token_progress(&handle), progress);

    // A new final changes the request row and its bucket, exactly once each.
    token_fact(
        &handle,
        job,
        micros + 2,
        Some(ELIGIBLE_FINAL),
        io5(Some(4), Some(4)),
    );
    let (report, writes) = commit_token_probed(&handle, 10);
    assert_eq!((report.requests_written, report.buckets_written), (1, 1));
    assert_eq!(writes, [1, 1, 0]);
    assert_eq!(
        token_bucket(&handle, provider, "model-a", micros).map(|t| t.total.final_sum),
        Some(8)
    );

    // Two plans from one snapshot: the second is stale and applies nothing.
    token_fact(
        &handle,
        job,
        micros + 3,
        Some(ELIGIBLE_FINAL),
        io5(Some(5), None),
    );
    token_fact(
        &handle,
        job,
        micros + 4,
        Some(ELIGIBLE_FINAL),
        [None, None, Some(1), None, None],
    );
    let first = token_plan(&handle, 1);
    let stale = token_plan(&handle, 2);
    assert!(wait(handle.commit_token(first)).expect("commit").is_some());
    let after_first = (
        token_progress(&handle),
        token_bucket(&handle, provider, "model-a", micros),
    );
    assert_eq!(wait(handle.commit_token(stale)).expect("commit"), None);
    assert_eq!(
        (
            token_progress(&handle),
            token_bucket(&handle, provider, "model-a", micros)
        ),
        after_first
    );
    assert_eq!(
        drain_tokens(&handle),
        1,
        "the stale batch's record is consumed once"
    );
    assert_matches_reducer(&handle, &[job]);

    // A guarded row that changed since preparation makes a plan stale even
    // when it would not have rewritten that row.
    token_fact(
        &handle,
        job,
        micros + 5,
        Some(ELIGIBLE),
        io5(Some(4), Some(4)),
    );
    let duplicate = token_plan(&handle, 10);
    assert_eq!(duplicate.requests_written(), 0);
    wait(handle.with_database(move |database| {
        crate::storage::replace_token_request(
            database,
            RequestRef {
                job,
                request: RequestNumber::FIRST,
            },
        )
    }))
    .expect("change the guarded request row");
    let progress = token_progress(&handle);
    assert_eq!(wait(handle.commit_token(duplicate)).expect("commit"), None);
    assert_eq!(token_progress(&handle), progress);

    // A bucket changed behind a plan's back makes it stale too.
    let moved = TokenMinuteTotals {
        requests: 3,
        ..TokenMinuteTotals::default()
    };
    let minute = minute_of(at(micros));
    let other = llm_job(&handle, llm);
    token_fact(&handle, other, micros, Some(ELIGIBLE), NO_COUNTERS);
    // Skip the duplicate record first, so the next plan covers the new Job.
    consume_tokens(&handle, 1);
    let plan = token_plan(&handle, 10);
    wait(handle.with_database(move |database| {
        crate::storage::replace_token_minute(database, provider, "model-a", minute, &moved)
    }))
    .expect("change the bucket");
    assert_eq!(wait(handle.commit_token(plan)).expect("commit"), None);
    assert_eq!(
        token_bucket(&handle, provider, "model-a", micros),
        Some(moved)
    );
    worker.shutdown().expect("shutdown");
}

#[test]
fn a_failed_token_commit_leaves_state_counts_and_progress_unchanged() {
    let (_root, directory) = data_directory("token-failed");
    let worker = DatabaseWorker::start(&directory).expect("start database");
    let handle = worker.handle();
    let (provider, llm) = llm_setup(&handle, "model-a");
    let job = llm_job(&handle, llm);
    let micros = at_minute(18);
    token_fact(&handle, job, micros, Some(ELIGIBLE), io5(Some(2), Some(3)));
    token_fact(
        &handle,
        job,
        micros + 1,
        Some(ELIGIBLE_FINAL),
        io5(Some(2), Some(3)),
    );

    let mut plan = token_plan(&handle, 10);
    plan.inject_failure = true;
    let error = wait(handle.commit_token(plan)).expect_err("the injected failure rolls back");
    assert!(
        matches!(error, DatabaseError::RolledBack(_)),
        "got {error:?}"
    );
    assert!(matches!(
        wait(handle.token_progress()),
        Err(DatabaseError::ReadsStorageFailed(_))
    ));
    assert!(matches!(
        worker.shutdown(),
        Err(DatabaseError::StorageUnavailable(_))
    ));

    let worker = DatabaseWorker::start(&directory).expect("reopen database");
    let handle = worker.handle();
    assert_eq!(token_progress(&handle), TokenAggregationProgress::default());
    assert_eq!(token_bucket(&handle, provider, "model-a", micros), None);
    assert!(
        wait(handle.token_request(RequestRef {
            job,
            request: RequestNumber::FIRST
        }))
        .expect("read")
        .value
        .is_none()
    );
    assert_eq!(drain_tokens(&handle), 2, "the records are still unconsumed");
    assert_matches_reducer(&handle, &[job]);
    worker.shutdown().expect("shutdown");
}

#[test]
fn stale_token_declines_in_a_group_are_not_flushed_or_uncertain() {
    use std::sync::atomic::Ordering::SeqCst;
    let (_root, directory) = data_directory("token-group");
    let worker = DatabaseWorker::start_with_config(
        &directory,
        crate::disk_protection::FilesystemConfig {
            disk_lock_threshold_bytes: 1,
            disk_resume_threshold_bytes: 2,
            ..crate::disk_protection::FilesystemConfig::default()
        },
    )
    .expect("start database");
    let handle = worker.handle();
    let (_provider, llm) = llm_setup(&handle, "model-a");
    let micros = at_minute(20);
    let (job, first_write, second_write) = (
        llm_job(&handle, llm),
        llm_job(&handle, llm),
        llm_job(&handle, llm),
    );
    token_fact(&handle, job, micros, Some(ELIGIBLE), NO_COUNTERS);
    assert_eq!(drain_tokens(&handle), 1);
    // Prepared while nothing is unconsumed: an empty plan.
    let empty = token_plan(&handle, 10);
    token_fact(
        &handle,
        job,
        micros + 1,
        Some(ELIGIBLE),
        io5(Some(1), Some(1)),
    );
    token_fact(
        &handle,
        job,
        micros + 2,
        Some(ELIGIBLE),
        io5(Some(2), Some(2)),
    );
    let stale_plan = || {
        let first = token_plan(&handle, 1);
        let stale = token_plan(&handle, 1);
        assert!(wait(handle.commit_token(first)).expect("commit").is_some());
        stale
    };

    // A group of declines makes no durable commit.
    let stale = stale_plan();
    let probe = handle.group_probe();
    let (held, release) = probe.hold_next_group();
    let flushes = probe.flushes.load(SeqCst);
    let markers_before = markers(&handle);
    let first = admit(handle.commit_token(stale));
    held.recv_timeout(crate::test_support::WAIT)
        .expect("the worker holds the group's first command");
    let second = admit(handle.commit_token(empty));
    release.send(()).expect("release the group");
    assert_eq!(first().expect("a stale commit declines"), None);
    assert_eq!(second().expect("an empty commit declines"), None);
    assert_eq!(
        probe.flushes.load(SeqCst),
        flushes,
        "a group that wrote nothing makes no durable commit"
    );
    assert_eq!(markers(&handle), markers_before);

    // A decline beside writes whose group commit is lost is rolled back, and
    // only the writes are reconciled as uncertain.
    token_fact(
        &handle,
        first_write,
        micros + 2,
        Some(ELIGIBLE),
        NO_COUNTERS,
    );
    token_fact(
        &handle,
        second_write,
        micros + 3,
        Some(ELIGIBLE),
        NO_COUNTERS,
    );
    let stale = stale_plan();
    let (held, release) = probe.hold_next_group();
    let declined = admit(handle.commit_token(stale));
    held.recv_timeout(crate::test_support::WAIT)
        .expect("the worker holds the group's first command");
    let write_one = admit(handle.record_availability_stat(NewAvailabilityStat {
        job: first_write,
        request: RequestNumber::FIRST,
        observed_at: at(micros + 4),
        observation: response(200),
    }));
    let write_two = admit(handle.record_availability_stat(NewAvailabilityStat {
        job: second_write,
        request: RequestNumber::FIRST,
        observed_at: at(micros + 5),
        observation: response(200),
    }));
    probe.fail_next_flush.store(true, SeqCst);
    release.send(()).expect("release the group");
    let unknown = |result: Result<AvailabilityStatId, DatabaseError>| match result {
        Err(DatabaseError::OutcomeUnknown { operation, .. }) => operation,
        other => panic!("a write in a lost group is unknown, got {other:?}"),
    };
    let mut written = vec![unknown(write_one()), unknown(write_two())];
    assert!(
        matches!(declined(), Err(DatabaseError::RolledBack(_))),
        "the declined Token commit committed nothing"
    );
    let report = wait(handle.recover_storage()).expect("recover storage");
    let mut reconciled: Vec<_> = report
        .reconciled
        .iter()
        .map(|(operation, _)| *operation)
        .collect();
    written.sort();
    reconciled.sort();
    assert_eq!(reconciled, written, "only the writes are uncertain");
    worker.shutdown().expect("shutdown");
}

#[test]
fn totals_that_would_leave_their_range_refuse_the_batch_without_writing() {
    let (_root, directory) = data_directory("token-range");
    let worker = DatabaseWorker::start(&directory).expect("start database");
    let handle = worker.handle();
    let (provider, llm) = llm_setup(&handle, "model-a");
    let micros = at_minute(22);
    let minute = minute_of(at(micros));
    let (job, other) = (llm_job(&handle, llm), llm_job(&handle, llm));
    token_fact(&handle, job, micros, Some(ELIGIBLE), NO_COUNTERS);
    consume_tokens(&handle, 10);

    // Overflow: a stored count at its maximum cannot take another request.
    let full = TokenMinuteTotals {
        requests: u64::MAX,
        ..TokenMinuteTotals::default()
    };
    wait(handle.with_database(move |database| {
        crate::storage::replace_token_minute(database, provider, "model-a", minute, &full)
    }))
    .expect("set the bucket");
    token_fact(&handle, other, micros, Some(ELIGIBLE), NO_COUNTERS);
    let progress = token_progress(&handle);
    assert!(
        wait(handle.consume_token_usage(10)).is_err(),
        "overflow is refused"
    );
    assert_eq!(token_progress(&handle), progress);
    assert_eq!(
        token_bucket(&handle, provider, "model-a", micros),
        Some(full)
    );
    assert!(
        wait(handle.token_request(RequestRef {
            job: other,
            request: RequestNumber::FIRST
        }))
        .expect("read")
        .value
        .is_none()
    );

    // Underflow: a bucket that no longer holds the request cannot give it up.
    wait(handle.with_database(move |database| {
        crate::storage::replace_token_minute(
            database,
            provider,
            "model-a",
            minute,
            &TokenMinuteTotals::default(),
        )
    }))
    .expect("empty the bucket");
    // `job` gives up its stored request first, from a bucket that has none.
    token_fact(
        &handle,
        job,
        micros + 1,
        Some(ELIGIBLE_FINAL),
        io5(Some(1), Some(1)),
    );
    let progress = token_progress(&handle);
    assert!(
        matches!(
            wait(handle.consume_token_usage(10)),
            Err(TokenConsumeError::Database(DatabaseError::Read(_)))
        ),
        "underflow is refused"
    );
    assert_eq!(token_progress(&handle), progress);
    assert_eq!(
        token_bucket(&handle, provider, "model-a", micros),
        Some(TokenMinuteTotals::default())
    );
    worker.shutdown().expect("shutdown");
}

// ---- the background aggregation service ------------------------------------------

use crate::orchestration::aggregation_service::{
    AggregationService, AggregationServiceConfig, EXECUTOR_NAME, FailedStart, JOB_NAME, JobEnd,
    RoundReport, ServiceFailure, ServiceStartError, ServiceState, StartCause, Stream,
    StreamOutcome, TestSeams,
};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering as AtomicOrdering};

/// Polls `condition` until it holds, within the shared bound.
fn eventually(mut condition: impl FnMut() -> bool) {
    let deadline = Instant::now() + crate::test_support::WAIT;
    while !condition() {
        assert!(
            Instant::now() < deadline,
            "the condition did not hold in time"
        );
        std::thread::sleep(Duration::from_millis(5));
    }
}

/// A running service whose checks the test steps by hand.
struct Rig {
    service: AggregationService,
    rounds: flume::Receiver<RoundReport>,
    consume_calls: Arc<AtomicUsize>,
}

impl Rig {
    fn start(handle: &DatabaseHandle, tweak: impl FnOnce(&mut TestSeams)) -> Self {
        let (sender, rounds) = flume::unbounded();
        let consume_calls = Arc::new(AtomicUsize::new(0));
        let mut seams = TestSeams {
            rounds: Some(sender),
            ..TestSeams::default()
        };
        tweak(&mut seams);
        let counted = seams.before_consume.take();
        let calls = consume_calls.clone();
        seams.before_consume = Some(Arc::new(move |stream| {
            calls.fetch_add(1, AtomicOrdering::SeqCst);
            if let Some(counted) = &counted {
                counted(stream);
            }
        }));
        let service = wait(AggregationService::start(
            handle.clone(),
            AggregationServiceConfig {
                interval: Duration::from_secs(3_600),
                check_immediately: false,
                seams,
            },
        ))
        .expect("start the service");
        Self {
            service,
            rounds,
            consume_calls,
        }
    }

    /// One check, run now, and what it did.
    fn tick(&self) -> RoundReport {
        assert!(self.service.tick(), "the loop is running");
        self.rounds
            .recv_timeout(crate::test_support::WAIT)
            .expect("the check finished")
    }

    fn calls(&self) -> usize {
        self.consume_calls.load(AtomicOrdering::SeqCst)
    }
}

fn outcomes(report: &RoundReport) -> Vec<(Stream, StreamOutcome)> {
    report.outcomes.clone()
}

fn service_jobs(handle: &DatabaseHandle) -> Vec<JobRecord> {
    wait(handle.jobs(None, 1_000))
        .expect("read Jobs")
        .into_iter()
        .filter(|job| job.name.as_deref() == Some(JOB_NAME))
        .collect()
}

fn aggregation_markers(handle: &DatabaseHandle) -> usize {
    markers(handle)
        .into_iter()
        .filter(|marker| {
            matches!(
                marker.kind,
                crate::persistence::OperationKind::ConsumeAvailability
                    | crate::persistence::OperationKind::ConsumeTokenUsage
            )
        })
        .count()
}

#[test]
fn startup_makes_one_started_unconfigured_service_job_and_batches_make_none() {
    let (_root, directory) = data_directory("service-startup");
    let worker = DatabaseWorker::start(&directory).expect("start database");
    let handle = worker.handle();
    let (_provider, llm) = llm_setup(&handle, "model-a");
    let job = llm_job(&handle, llm);
    observe(&handle, job, at_minute(1), response(200));
    token_fact(&handle, job, at_minute(1), Some(ELIGIBLE), NO_COUNTERS);
    let jobs_before = wait(handle.jobs(None, 1_000)).expect("read Jobs").len();

    let rig = Rig::start(&handle, |_| {});
    let services = service_jobs(&handle);
    assert_eq!(services.len(), 1, "exactly one service Job for this run");
    let service = &services[0];
    assert_eq!(service.id, rig.service.job());
    assert_eq!(service.execution_mode, ExecutionMode::Continuous);
    assert!(service.started_at.is_some());
    assert!(
        service.completed_at.is_none()
            && service.failed_at.is_none()
            && service.unknown_at.is_none(),
        "it is running"
    );
    assert_eq!(
        (service.parent, service.configuration),
        (handle.genesis(), None)
    );
    assert!(service.projects.is_empty(), "no special Project");
    let executor = wait(handle.executor(service.assigned_executor.expect("assigned")))
        .expect("read Executor")
        .expect("Executor exists");
    assert_eq!(
        (executor.name.as_str(), executor.executor_type),
        (EXECUTOR_NAME, ExecutorType::System)
    );
    assert_eq!(
        blocking(&handle, llm),
        [job],
        "the service holds no LLM Profile; only the fixture Job does"
    );

    // Both streams are consumed by the service, and no Job comes of a batch.
    assert_eq!(rig.tick().outcomes.len(), 2);
    assert!(token_progress(&handle).consumed_through.is_some());
    assert!(
        wait(handle.availability_progress())
            .expect("progress")
            .consumed_through
            .is_some()
    );
    assert_eq!(
        wait(handle.jobs(None, 1_000)).expect("read Jobs").len(),
        jobs_before + 1,
        "batches are not Jobs"
    );
    assert!(
        wait(handle.job_outputs(service.id, None, 10))
            .expect("read outputs")
            .is_empty(),
        "no heartbeat or per-check output"
    );

    // A second host over the same opening is refused while this one runs.
    assert!(matches!(
        wait(AggregationService::start(
            handle.clone(),
            AggregationServiceConfig::default()
        )),
        Err(ServiceStartError::AlreadyRunning)
    ));
    wait(rig.service.shutdown())
        .into_result()
        .expect("a clean stop");
    worker.shutdown().expect("shutdown");
}

#[test]
fn the_service_consumes_both_streams_without_anyone_calling_a_consume_method() {
    let (_root, directory) = data_directory("service-automatic");
    let worker = DatabaseWorker::start(&directory).expect("start database");
    let handle = worker.handle();
    let (provider, llm) = llm_setup(&handle, "model-a");
    let job = llm_job(&handle, llm);
    let micros = at_minute(4);
    observe(&handle, job, micros, response(200));
    token_fact(
        &handle,
        job,
        micros,
        Some(ELIGIBLE_FINAL),
        io5(Some(3), Some(4)),
    );

    let (sender, rounds) = flume::unbounded();
    let service = wait(AggregationService::start(
        handle.clone(),
        AggregationServiceConfig {
            interval: Duration::from_millis(20),
            check_immediately: true,
            seams: TestSeams {
                rounds: Some(sender),
                ..TestSeams::default()
            },
        },
    ))
    .expect("start the service");
    // The first check ran before the start returned; later ones come by timer.
    assert!(service.checks() >= 1);
    eventually(|| rounds.try_iter().count() > 0 || service.checks() >= 2);
    assert_eq!(counts(&handle, provider, "model-a", micros).available, 1);
    assert_eq!(
        token_bucket(&handle, provider, "model-a", micros).map(|totals| totals.total.final_sum),
        Some(7)
    );
    assert_eq!(service.state(), ServiceState::Running);
    wait(service.shutdown())
        .into_result()
        .expect("a clean stop");
    worker.shutdown().expect("shutdown");
}

#[test]
fn a_backlog_is_taken_a_batch_at_a_time_and_the_streams_take_turns() {
    let (_root, directory) = data_directory("service-backlog");
    let worker = DatabaseWorker::start(&directory).expect("start database");
    let handle = worker.handle();
    let (_provider, llm) = llm_setup(&handle, "model-a");
    let job = llm_job(&handle, llm);
    // 1,200 generic Token records and 1,200 availability observations.
    for chunk in 0..12 {
        let tokens: Vec<_> = (0..100)
            .map(|offset| {
                admit(handle.record_metrics(NewMetrics {
                    job,
                    request: RequestNumber::FIRST,
                    observed_at: at(at_minute(2) + chunk * 100 + offset),
                    measurements: TokenMeasurements {
                        usage: NormalizedUsage {
                            input_tokens: Some(1),
                            ..NormalizedUsage::default()
                        },
                        ..TokenMeasurements::default()
                    },
                    token: None,
                }))
            })
            .collect();
        let stats: Vec<_> = (0..100)
            .map(|offset| {
                admit(handle.record_availability_stat(NewAvailabilityStat {
                    job,
                    request: RequestNumber::FIRST,
                    observed_at: at(at_minute(2) + chunk * 100 + offset),
                    observation: bare(200),
                }))
            })
            .collect();
        for wait_for in tokens {
            wait_for().expect("record Metrics");
        }
        for wait_for in stats {
            wait_for().expect("record AvailabilityStat");
        }
    }

    let rig = Rig::start(&handle, |_| {});
    let first = rig.tick();
    assert_eq!(
        outcomes(&first),
        [
            (Stream::Availability, StreamOutcome::Consumed(1_000)),
            (Stream::Token, StreamOutcome::Consumed(1_000)),
        ],
        "each stream gets one bounded batch"
    );
    assert_eq!(
        token_progress(&handle).consumed_through.map(|id| id.get()),
        Some(1_000)
    );
    assert_eq!(rig.calls(), 2, "one consume call per stream");
    let second = rig.tick();
    assert_eq!(
        outcomes(&second),
        [
            (Stream::Token, StreamOutcome::Consumed(200)),
            (Stream::Availability, StreamOutcome::Consumed(200)),
        ],
        "the first opportunity alternates, and skipped Token records still advance"
    );
    assert_eq!(
        token_progress(&handle).consumed_through.map(|id| id.get()),
        Some(1_200)
    );
    let third = rig.tick();
    assert_eq!(
        outcomes(&third),
        [
            (Stream::Availability, StreamOutcome::Idle),
            (Stream::Token, StreamOutcome::Idle),
        ]
    );

    // Idle checks write nothing: no marker, progress, update time or row.
    let idle_markers = aggregation_markers(&handle);
    let progress = (
        token_progress(&handle),
        wait(handle.availability_progress()).expect("progress"),
    );
    for _ in 0..5 {
        rig.tick();
    }
    assert_eq!(aggregation_markers(&handle), idle_markers);
    assert_eq!(
        (
            token_progress(&handle),
            wait(handle.availability_progress()).expect("progress")
        ),
        progress
    );
    assert_eq!(
        wait(handle.job_outputs(rig.service.job(), None, 10))
            .expect("outputs")
            .len(),
        0
    );
    wait(rig.service.shutdown())
        .into_result()
        .expect("a clean stop");
    worker.shutdown().expect("shutdown");
}

#[test]
fn contention_defers_to_a_later_check_and_never_starves_the_other_stream() {
    let (_root, directory) = data_directory("service-contended");
    let worker = DatabaseWorker::start(&directory).expect("start database");
    let handle = worker.handle();
    let (provider, llm) = llm_setup(&handle, "model-a");
    let job = llm_job(&handle, llm);
    observe(&handle, job, at_minute(3), response(200));
    token_fact(&handle, job, at_minute(3), Some(ELIGIBLE), NO_COUNTERS);

    let contend_once = Arc::new(AtomicBool::new(true));
    let rig = Rig::start(&handle, |seams| {
        let contend_once = contend_once.clone();
        seams.outcome_override = Some(Arc::new(move |stream| {
            (stream == Stream::Availability && contend_once.swap(false, AtomicOrdering::SeqCst))
                .then_some(StreamOutcome::Contended)
        }));
    });
    assert_eq!(
        outcomes(&rig.tick()),
        [
            (Stream::Availability, StreamOutcome::Contended),
            (Stream::Token, StreamOutcome::Consumed(1)),
        ],
        "a contended stream does not cost the other its turn"
    );
    assert_eq!(
        counts(&handle, provider, "model-a", at_minute(3)).available,
        0
    );
    assert_eq!(
        outcomes(&rig.tick()),
        [
            (Stream::Token, StreamOutcome::Idle),
            (Stream::Availability, StreamOutcome::Consumed(1)),
        ],
        "and it is tried again on the next check"
    );
    assert_eq!(
        counts(&handle, provider, "model-a", at_minute(3)).available,
        1
    );
    assert_eq!(rig.service.state(), ServiceState::Running);
    wait(rig.service.shutdown())
        .into_result()
        .expect("a clean stop");
    worker.shutdown().expect("shutdown");
}

#[test]
fn protected_storage_pauses_the_service_and_recovery_resumes_from_committed_progress() {
    use crate::disk_protection::{ProtectedArea, ProtectionReason, SpaceShortage};
    let (_root, directory) = data_directory("service-protected");
    let worker = DatabaseWorker::start_with_config(
        &directory,
        crate::disk_protection::FilesystemConfig {
            disk_lock_threshold_bytes: 1,
            disk_resume_threshold_bytes: 2,
            ..crate::disk_protection::FilesystemConfig::default()
        },
    )
    .expect("start database");
    let handle = worker.handle();
    let (provider, llm) = llm_setup(&handle, "model-a");
    let job = llm_job(&handle, llm);
    let micros = at_minute(6);
    observe(&handle, job, micros, response(200));
    token_fact(
        &handle,
        job,
        micros,
        Some(ELIGIBLE_FINAL),
        io5(Some(2), Some(3)),
    );
    let rig = Rig::start(&handle, |_| {});

    let reason = ProtectionReason::InsufficientSpace(SpaceShortage {
        area: ProtectedArea::Database,
        path: directory.database_path().to_path_buf(),
        available_bytes: 0,
        expected_write_bytes: 0,
        threshold_bytes: 1,
    });
    assert!(handle.activate_filesystem_protection(reason));
    let before = aggregation_markers(&handle);
    for _ in 0..3 {
        let report = rig.tick();
        assert_eq!(report.outcomes.len(), 1);
        assert_eq!(report.outcomes[0].1, StreamOutcome::Paused);
    }
    assert_eq!(rig.calls(), 0, "no consume, and so no write, was attempted");
    assert_eq!(rig.service.state(), ServiceState::Paused);
    assert_eq!(aggregation_markers(&handle), before);

    // Existing recovery, run by its own actor, reopens the database; the
    // service then resumes through the ordinary consume methods.
    wait(handle.recover_storage()).expect("recover storage");
    let report = rig.tick();
    assert!(
        report
            .outcomes
            .iter()
            .all(|(_, outcome)| matches!(outcome, StreamOutcome::Consumed(1)))
    );
    assert_eq!(rig.service.state(), ServiceState::Running);
    assert_eq!(counts(&handle, provider, "model-a", micros).available, 1);
    assert_eq!(
        token_bucket(&handle, provider, "model-a", micros).map(|totals| totals.requests),
        Some(1),
        "nothing was applied twice"
    );
    assert_matches_reducer(&handle, &[job]);
    wait(rig.service.shutdown())
        .into_result()
        .expect("a clean stop");
    worker.shutdown().expect("shutdown");
}

#[test]
fn contradictory_token_evidence_halts_only_the_token_stream_without_skipping_or_spinning() {
    let (_root, directory) = data_directory("service-evidence");
    let worker = DatabaseWorker::start(&directory).expect("start database");
    let handle = worker.handle();
    let (_provider, llm) = llm_setup(&handle, "model-a");
    let (bad, good) = (llm_job(&handle, llm), llm_job(&handle, llm));
    let excluded = token_fact(
        &handle,
        bad,
        at_minute(8),
        Some(TokenAnnotation::excluded()),
        NO_COUNTERS,
    );
    let rig = Rig::start(&handle, |_| {});
    rig.tick();
    let progress = token_progress(&handle);
    assert_eq!(progress.consumed_through, Some(excluded));

    token_fact(&handle, good, at_minute(8), Some(ELIGIBLE), NO_COUNTERS);
    // Ingest refuses the contradiction now; an older database may hold it.
    preexisting_fact(
        &handle,
        bad,
        at_minute(8),
        Some(ELIGIBLE),
        io5(Some(1), Some(1)),
    );
    let report = rig.tick();
    assert!(
        outcomes(&report).contains(&(Stream::Token, StreamOutcome::Halted)),
        "{report:?}"
    );
    assert!(matches!(rig.service.state(), ServiceState::Degraded(_)));

    // Later checks never retry the Token stream, and Availability goes on.
    let calls = rig.calls();
    observe(&handle, good, at_minute(8), response(200));
    let mut consumed = 0;
    for _ in 0..3 {
        for (stream, outcome) in outcomes(&rig.tick()) {
            match stream {
                Stream::Token => assert_eq!(outcome, StreamOutcome::Halted),
                Stream::Availability => {
                    if let StreamOutcome::Consumed(records) = outcome {
                        consumed += records;
                    }
                }
            }
        }
    }
    assert_eq!(consumed, 1, "the observation was consumed once");
    assert_eq!(
        rig.calls(),
        calls + 3,
        "only Availability was attempted, once per check"
    );
    assert_eq!(
        token_progress(&handle),
        progress,
        "the consumed prefix is intact"
    );
    assert_eq!(
        token_sample(&handle, good),
        None,
        "and nothing from the refused batch"
    );

    let service = rig.service;
    let job = service.job();
    let outcome = wait(service.shutdown());
    assert!(matches!(outcome.failure, Some(ServiceFailure::Evidence(_))));
    assert!(
        matches!(outcome.end, JobEnd::Recorded),
        "the failure is recorded once the loop has stopped"
    );
    let record = wait(handle.job(job))
        .expect("read Job")
        .expect("Job exists");
    assert!(record.failed_at.is_some() && record.completed_at.is_none());
    worker.shutdown().expect("shutdown");
}

#[test]
fn corruption_stops_the_service_and_is_reported() {
    let (_root, directory) = data_directory("service-corruption");
    let worker = DatabaseWorker::start(&directory).expect("start database");
    let handle = worker.handle();
    let (_provider, llm) = llm_setup(&handle, "model-a");
    let job = llm_job(&handle, llm);
    token_fact(&handle, job, at_minute(9), Some(ELIGIBLE), NO_COUNTERS);
    let rig = Rig::start(&handle, |_| {});
    wait(handle.with_database(move |database| crate::storage::remove_job_row(database, job)))
        .expect("corrupt the Job row");
    assert!(rig.service.tick());
    eventually(|| matches!(rig.service.state(), ServiceState::Failed(_)));
    assert_eq!(
        token_progress(&handle).consumed_through,
        None,
        "no record was skipped"
    );
    let outcome = wait(rig.service.shutdown());
    assert!(matches!(
        outcome.failure,
        Some(ServiceFailure::Database {
            stream: Stream::Token,
            ..
        }) | Some(ServiceFailure::Database {
            stream: Stream::Availability,
            ..
        })
    ));
    worker.shutdown().ok();
}

#[test]
fn stopping_wakes_the_timer_waits_for_a_batch_in_flight_and_only_then_ends_the_job() {
    let (_root, directory) = data_directory("service-stop");
    let worker = DatabaseWorker::start(&directory).expect("start database");
    let handle = worker.handle();
    let (provider, llm) = llm_setup(&handle, "model-a");
    let job = llm_job(&handle, llm);

    // An idle loop waiting on an hour-long timer stops at once.
    let idle = Rig::start(&handle, |_| {});
    let started = Instant::now();
    let service_job = idle.service.job();
    wait(idle.service.shutdown())
        .into_result()
        .expect("a clean stop");
    assert!(
        started.elapsed() < Duration::from_secs(5),
        "the waiting timer was woken"
    );
    let record = wait(handle.job(service_job))
        .expect("read Job")
        .expect("Job exists");
    assert!(record.completed_at.is_some());

    // A batch held in flight is waited for, and its Job ends after it.
    token_fact(&handle, job, at_minute(11), Some(ELIGIBLE), NO_COUNTERS);
    let (entered_sender, entered) = flume::unbounded();
    let (release, released) = flume::unbounded::<()>();
    let rig = Rig::start(&handle, |seams| {
        seams.before_consume = Some(Arc::new(move |stream| {
            if stream == Stream::Token {
                entered_sender.send(()).ok();
                released.recv_timeout(crate::test_support::WAIT).ok();
            }
        }));
    });
    let service_job = rig.service.job();
    assert!(rig.service.tick());
    entered
        .recv_timeout(crate::test_support::WAIT)
        .expect("the Token batch is in flight");
    let finished = Arc::new(AtomicBool::new(false));
    let stopper = {
        let finished = finished.clone();
        let service = rig.service;
        std::thread::spawn(move || {
            let outcome = wait(service.shutdown());
            finished.store(true, AtomicOrdering::SeqCst);
            outcome
        })
    };
    assert!(
        !finished.load(AtomicOrdering::SeqCst),
        "the stop does not finish while a batch is in flight"
    );
    release.send(()).expect("release the batch");
    let outcome = stopper.join().expect("the stopper thread");
    outcome.into_result().expect("a clean stop");

    assert!(
        token_progress(&handle).consumed_through.is_some(),
        "the held batch completed"
    );
    assert_eq!(
        token_bucket(&handle, provider, "model-a", at_minute(11)).map(|totals| totals.requests),
        Some(1)
    );
    let record = wait(handle.job(service_job))
        .expect("read Job")
        .expect("Job exists");
    assert!(record.completed_at.is_some());
    let all = markers(&handle);
    let last_batch = all
        .iter()
        .rposition(|marker| marker.kind == crate::persistence::OperationKind::ConsumeTokenUsage)
        .expect("a Token batch marker");
    let end = all
        .iter()
        .rposition(|marker| {
            marker.subject == RecordRef::Job(service_job)
                && marker.kind == crate::persistence::OperationKind::FinalizeJob
        })
        .expect("the Job's end marker");
    assert!(
        last_batch < end,
        "the Job ends after the batch, and nothing is written after"
    );
    assert_eq!(end, all.len() - 1);
    worker.shutdown().expect("shutdown");
}

#[test]
fn a_restart_keeps_the_aggregates_and_progress_and_makes_a_new_service_job() {
    let (_root, directory) = data_directory("service-restart");
    let worker = DatabaseWorker::start(&directory).expect("start database");
    let handle = worker.handle();
    let (provider, llm) = llm_setup(&handle, "model-a");
    let job = llm_job(&handle, llm);
    observe(&handle, job, at_minute(12), response(200));
    token_fact(&handle, job, at_minute(12), Some(ELIGIBLE), NO_COUNTERS);

    // A run that ended without a record of its end: its Job stays started.
    let crashed = Rig::start(&handle, |_| {});
    crashed.tick();
    let crashed_job = crashed.service.job();
    drop(crashed);
    let progress = (
        token_progress(&handle),
        wait(handle.availability_progress()).expect("progress"),
    );
    let bucket = token_bucket(&handle, provider, "model-a", at_minute(12));

    let other = llm_job(&handle, llm);
    observe(&handle, other, at_minute(13), response(200));
    token_fact(&handle, other, at_minute(13), Some(ELIGIBLE), NO_COUNTERS);
    let rig = Rig::start(&handle, |_| {});
    assert_ne!(
        rig.service.job(),
        crashed_job,
        "each run has its own service Job"
    );
    assert_eq!(
        (
            token_progress(&handle),
            wait(handle.availability_progress()).expect("progress")
        ),
        progress,
        "starting resets neither progress"
    );
    assert_eq!(
        token_bucket(&handle, provider, "model-a", at_minute(12)),
        bucket
    );
    assert_eq!(
        outcomes(&rig.tick()),
        [
            (Stream::Availability, StreamOutcome::Consumed(1)),
            (Stream::Token, StreamOutcome::Consumed(1)),
        ],
        "only the new raw records are processed"
    );
    let older = wait(handle.job(crashed_job))
        .expect("read Job")
        .expect("Job exists");
    assert!(
        older.started_at.is_some()
            && older.completed_at.is_none()
            && older.failed_at.is_none()
            && older.unknown_at.is_none(),
        "an unconfirmed older run is not marked anything"
    );
    let executors = wait(handle.executors(None, 100)).expect("read Executors");
    assert_eq!(
        executors
            .iter()
            .filter(|executor| executor.name == EXECUTOR_NAME)
            .count(),
        1,
        "the System Executor is reused, not registered again"
    );
    wait(rig.service.shutdown())
        .into_result()
        .expect("a clean stop");
    assert_eq!(service_jobs(&handle).len(), 2);
    worker.shutdown().expect("shutdown");
}

// ---- first-check failures, latched storage and the host's cleanup ------------------

/// A Job's service record, read back.
fn job_record(handle: &DatabaseHandle, job: JobId) -> JobRecord {
    wait(handle.job(job))
        .expect("read Job")
        .expect("Job exists")
}

fn failed_start(result: Result<AggregationService, ServiceStartError>) -> Box<FailedStart> {
    match result {
        Err(ServiceStartError::Failed(failed)) => failed,
        Err(other) => panic!("expected a failed start with a service Job, got {other:?}"),
        Ok(_) => panic!("expected the first check to fail the start"),
    }
}

fn is_started_only(record: &JobRecord) -> bool {
    record.started_at.is_some()
        && record.completed_at.is_none()
        && record.failed_at.is_none()
        && record.cancelled_at.is_none()
        && record.unknown_at.is_none()
}

/// A contradictory eligibility fixture: the Excluded claim comes first. Ingest
/// refuses the second claim now, so it is written as an older database held it.
fn contradictory(
    handle: &DatabaseHandle,
    llm: ProfileId,
) -> (JobId, crate::persistence::MetricsId) {
    let bad = llm_job(handle, llm);
    token_fact(
        handle,
        bad,
        at_minute(30),
        Some(TokenAnnotation::excluded()),
        NO_COUNTERS,
    );
    let eligible = preexisting_fact(
        handle,
        bad,
        at_minute(30),
        Some(ELIGIBLE),
        io5(Some(1), Some(1)),
    );
    (bad, eligible)
}

/// A first check that fails the whole loop: one Token fact whose minute
/// bucket cannot take another request.
fn corrupted_token_bucket(
    handle: &DatabaseHandle,
    provider: LlmProviderId,
    llm: ProfileId,
) -> JobId {
    let job = llm_job(handle, llm);
    let micros = at_minute(30);
    token_fact(handle, job, micros, Some(ELIGIBLE), NO_COUNTERS);
    let minute = minute_of(at(micros));
    let full = TokenMinuteTotals {
        requests: u64::MAX,
        ..TokenMinuteTotals::default()
    };
    wait(handle.with_database(move |database| {
        crate::storage::replace_token_minute(database, provider, "model-a", minute, &full)
    }))
    .expect("break the fixture");
    job
}

#[test]
fn a_first_check_on_invalid_token_evidence_halts_only_token_and_ends_the_run_as_failed() {
    let (_root, directory) = data_directory("service-first-evidence");
    let worker = DatabaseWorker::start(&directory).expect("start database");
    let handle = worker.handle();
    let (_provider, llm) = llm_setup(&handle, "model-a");
    let (bad, eligible) = contradictory(&handle, llm);
    let observed = llm_job(&handle, llm);
    observe(&handle, observed, at_minute(30), response(200));

    // The service starts, and its first check still consumes Availability.
    let service = wait(AggregationService::start(
        handle.clone(),
        AggregationServiceConfig::default(),
    ))
    .expect("invalid Token evidence does not stop the service");
    assert!(
        matches!(service.state(), ServiceState::Degraded(reason) if reason.contains(&bad.to_string())),
        "{:?}",
        service.state()
    );
    let availability = wait(handle.job_availability(observed, None, 10)).expect("read");
    assert_eq!(
        wait(handle.availability_progress())
            .expect("progress")
            .consumed_through,
        availability.last().map(|record| record.id),
        "Availability went on"
    );
    assert_eq!(
        token_progress(&handle).consumed_through,
        None,
        "no Token record was skipped"
    );
    assert_eq!(
        job_metrics(&handle, bad).len(),
        2,
        "the raw facts are intact"
    );

    // Stopping reports the concrete cause and ends the run as Failed.
    let job = service.job();
    let outcome = wait(service.shutdown());
    match &outcome.failure {
        Some(ServiceFailure::Evidence(TokenConsumeError::Evidence { metrics, job, .. })) => {
            assert_eq!((*metrics, *job), (eligible, bad))
        }
        other => panic!("the concrete cause is kept, got {other:?}"),
    }
    assert!(matches!(outcome.end, JobEnd::Recorded));
    let record = job_record(&handle, job);
    assert!(
        record.started_at.is_some() && record.failed_at.is_some() && record.completed_at.is_none(),
        "the run is Failed, not left Started"
    );

    // Another run, under its own Job, meets the same fact the same way.
    let again = wait(AggregationService::start(
        handle.clone(),
        AggregationServiceConfig::default(),
    ))
    .expect("start again");
    assert_ne!(again.job(), job);
    assert!(matches!(again.state(), ServiceState::Degraded(_)));
    assert!(wait(again.shutdown()).failure.is_some());
    assert_eq!(service_jobs(&handle).len(), 2);
    assert_eq!(token_progress(&handle).consumed_through, None);
    worker.shutdown().expect("shutdown");
}

#[test]
fn a_corrupted_first_check_is_reported_and_a_repaired_fixture_starts() {
    let (_root, directory) = data_directory("service-first-corruption");
    let worker = DatabaseWorker::start(&directory).expect("start database");
    let handle = worker.handle();
    let (provider, llm) = llm_setup(&handle, "model-a");
    let job = llm_job(&handle, llm);
    let micros = at_minute(31);
    token_fact(&handle, job, micros, Some(ELIGIBLE), NO_COUNTERS);
    let minute = minute_of(at(micros));
    let full = TokenMinuteTotals {
        requests: u64::MAX,
        ..TokenMinuteTotals::default()
    };
    wait(handle.with_database(move |database| {
        crate::storage::replace_token_minute(database, provider, "model-a", minute, &full)
    }))
    .expect("break the fixture");

    let failed = failed_start(wait(AggregationService::start(
        handle.clone(),
        AggregationServiceConfig::default(),
    )));
    match &failed.cause {
        StartCause::Loop(ServiceFailure::Database {
            stream: Stream::Token,
            error,
        }) => assert!(
            matches!(error, DatabaseError::Read(error) if error.kind == crate::database::StorageErrorKind::Corrupted),
            "the corruption cause is kept: {error:?}"
        ),
        other => panic!("expected the corruption cause, got {other:?}"),
    }
    assert!(matches!(failed.end, JobEnd::Recorded));
    assert!(job_record(&handle, failed.job).failed_at.is_some());
    assert_eq!(token_progress(&handle).consumed_through, None);
    assert_eq!(
        token_bucket(&handle, provider, "model-a", micros),
        Some(full),
        "the bucket is as found"
    );

    // Only the test fixture is repaired; the host starts again from the same
    // durable state.
    wait(handle.with_database(move |database| {
        crate::storage::replace_token_minute(
            database,
            provider,
            "model-a",
            minute,
            &TokenMinuteTotals::default(),
        )
    }))
    .expect("repair the fixture");
    let service = wait(AggregationService::start(
        handle.clone(),
        AggregationServiceConfig::default(),
    ))
    .expect("the repaired fixture starts");
    assert_ne!(service.job(), failed.job);
    assert_eq!(
        token_bucket(&handle, provider, "model-a", micros).map(|totals| totals.requests),
        Some(1),
        "the first check consumed the fact once"
    );
    wait(service.shutdown())
        .into_result()
        .expect("a clean stop");
    worker.shutdown().expect("shutdown");
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

#[test]
fn a_run_whose_end_is_refused_keeps_both_the_failure_and_the_refusal() {
    let (_root, directory) = data_directory("service-end-refused");
    let worker = DatabaseWorker::start(&directory).expect("start database");
    let handle = worker.handle();
    let (provider, llm) = llm_setup(&handle, "model-a");
    corrupted_token_bucket(&handle, provider, llm);
    let (hook_handle, reason) = (handle.clone(), protection_reason(&directory));
    let failed = failed_start(wait(AggregationService::start(
        handle.clone(),
        AggregationServiceConfig {
            seams: TestSeams {
                // Protection arrives while the first check is under way.
                before_consume: Some(Arc::new(move |stream| {
                    if stream == Stream::Token {
                        assert!(hook_handle.activate_filesystem_protection(reason.clone()));
                    }
                })),
                ..TestSeams::default()
            },
            ..AggregationServiceConfig::default()
        },
    )));
    assert!(matches!(
        failed.cause,
        StartCause::Loop(ServiceFailure::Database {
            stream: Stream::Token,
            ..
        })
    ));
    assert!(
        matches!(
            &failed.end,
            JobEnd::NotRecorded(DatabaseError::DiskProtected(_))
        ),
        "a positively refused end is not an unknown one: {:?}",
        failed.end
    );
    let text = failed.to_string();
    assert!(
        text.contains("still started") && !text.contains("unconfirmed"),
        "{text}"
    );
    assert!(
        is_started_only(&job_record(&handle, failed.job)),
        "nothing was committed, so the Job is still started"
    );
    worker.shutdown().expect("shutdown");
}

#[test]
fn a_run_whose_end_has_an_unknown_outcome_is_reported_as_unconfirmed() {
    let (_root, directory) = data_directory("service-end-unknown");
    let worker = DatabaseWorker::start(&directory).expect("start database");
    let handle = worker.handle();
    let (provider, llm) = llm_setup(&handle, "model-a");
    let bad = corrupted_token_bucket(&handle, provider, llm);
    let (stash, held_and_release) = flume::unbounded();
    let hook_handle = handle.clone();
    let config = AggregationServiceConfig {
        seams: TestSeams {
            // The Job's end is the worker's next command: hold it so a second
            // command joins its group, and make the group's flush fail.
            before_consume: Some(Arc::new(move |stream| {
                if stream == Stream::Token {
                    stash.send(hook_handle.group_probe().hold_next_group()).ok();
                }
            })),
            ..TestSeams::default()
        },
        ..AggregationServiceConfig::default()
    };
    let starter = {
        let handle = handle.clone();
        std::thread::spawn(move || wait(AggregationService::start(handle, config)))
    };
    let (held, release) = held_and_release
        .recv_timeout(crate::test_support::WAIT)
        .expect("the first check armed the hold");
    held.recv_timeout(crate::test_support::WAIT)
        .expect("the worker holds the Job's end");
    let companion = admit(handle.record_metrics(NewMetrics {
        job: bad,
        request: RequestNumber::FIRST,
        observed_at: at(at_minute(32)),
        measurements: TokenMeasurements {
            ttfb_ms: Some(1),
            ..TokenMeasurements::default()
        },
        token: None,
    }));
    handle
        .group_probe()
        .fail_next_flush
        .store(true, AtomicOrdering::SeqCst);
    release.send(()).expect("release the group");
    let failed = failed_start(starter.join().expect("starter thread"));
    assert!(matches!(
        companion(),
        Err(DatabaseError::OutcomeUnknown { .. })
    ));
    assert!(matches!(
        failed.cause,
        StartCause::Loop(ServiceFailure::Database {
            stream: Stream::Token,
            ..
        })
    ));
    assert!(
        matches!(
            &failed.end,
            JobEnd::Unconfirmed(DatabaseError::OutcomeUnknown { .. })
        ),
        "{:?}",
        failed.end
    );
    let text = failed.to_string();
    assert!(
        text.contains("unconfirmed") && !text.contains("still started"),
        "{text}"
    );
    worker.shutdown().ok();
}

fn recovering_worker(directory: &DataDirectory) -> DatabaseWorker {
    DatabaseWorker::start_with_config(
        directory,
        crate::disk_protection::FilesystemConfig {
            disk_lock_threshold_bytes: 1,
            disk_resume_threshold_bytes: 2,
            ..crate::disk_protection::FilesystemConfig::default()
        },
    )
    .expect("start database")
}

/// Pauses are not repeated attempts: further checks neither consume nor write.
fn assert_paused_without_attempts(rig: &Rig, handle: &DatabaseHandle) {
    let calls = rig.calls();
    assert_ne!(handle.status(), crate::database::DatabaseStatus::Open);
    assert_eq!(rig.service.state(), ServiceState::Paused);
    for _ in 0..3 {
        let report = rig.tick();
        assert_eq!(report.outcomes.len(), 1);
        assert_eq!(report.outcomes[0].1, StreamOutcome::Paused);
    }
    assert_eq!(
        rig.calls(),
        calls,
        "no consume call, and so no write, was repeated"
    );
}

#[test]
fn a_rolled_back_commit_pauses_the_live_service_and_recovery_lets_it_resume() {
    let (_root, directory) = data_directory("service-rolled-back");
    let worker = recovering_worker(&directory);
    let handle = worker.handle();
    let (provider, llm) = llm_setup(&handle, "model-a");
    let job = llm_job(&handle, llm);
    let micros = at_minute(33);
    token_fact(
        &handle,
        job,
        micros,
        Some(ELIGIBLE_FINAL),
        io5(Some(2), Some(3)),
    );
    let rig = Rig::start(&handle, |_| {});
    let service_job = rig.service.job();

    // A real failed commit of the service's own batch: storage latches.
    handle
        .group_probe()
        .fail_next_plan
        .store(true, AtomicOrdering::SeqCst);
    let report = rig.tick();
    assert_eq!(
        outcomes(&report),
        [
            (Stream::Availability, StreamOutcome::Idle),
            (Stream::Token, StreamOutcome::Paused),
        ]
    );
    assert_paused_without_attempts(&rig, &handle);

    // An external actor runs the existing recovery. A rollback is not an
    // uncertain commit, so nothing needs reconciling.
    let recovery = wait(handle.recover_storage()).expect("recover storage");
    assert!(recovery.reconciled.is_empty());
    assert_eq!(
        token_progress(&handle).consumed_through,
        None,
        "nothing was committed"
    );
    assert!(
        is_started_only(&job_record(&handle, service_job)),
        "the service Job is still running"
    );
    let resumed = rig.tick();
    assert!(
        resumed
            .outcomes
            .iter()
            .any(|outcome| outcome == &(Stream::Token, StreamOutcome::Consumed(1))),
        "{resumed:?}"
    );
    assert_eq!(rig.service.state(), ServiceState::Running);
    assert_eq!(
        token_bucket(&handle, provider, "model-a", micros).map(|totals| totals.requests),
        Some(1),
        "the fact is counted once, and was not skipped"
    );
    assert_matches_reducer(&handle, &[job]);
    wait(rig.service.shutdown())
        .into_result()
        .expect("a clean stop");
    worker.shutdown().expect("shutdown");
}

#[test]
fn an_uncertain_commit_pauses_the_live_service_and_resumes_from_what_was_committed() {
    use crate::database::OperationStatus;
    let (_root, directory) = data_directory("service-unknown");
    let worker = recovering_worker(&directory);
    let handle = worker.handle();
    let (provider, llm) = llm_setup(&handle, "model-a");
    let job = llm_job(&handle, llm);
    let micros = at_minute(34);
    token_fact(
        &handle,
        job,
        micros,
        Some(ELIGIBLE_FINAL),
        io5(Some(2), Some(3)),
    );
    let rig = Rig::start(&handle, |_| {});
    let service_job = rig.service.job();

    // The service's commit is the first command of a group whose durable
    // flush then really fails, so the commit's outcome is unknown.
    let probe = handle.group_probe();
    let (held, release) = probe.hold_next_group();
    assert!(rig.service.tick());
    held.recv_timeout(crate::test_support::WAIT)
        .expect("the service's commit is the group's first command");
    let companion = admit(handle.record_availability_stat(NewAvailabilityStat {
        job,
        request: RequestNumber::FIRST,
        observed_at: at(micros),
        observation: response(200),
    }));
    probe.fail_next_flush.store(true, AtomicOrdering::SeqCst);
    release.send(()).expect("release the group");
    let report = rig
        .rounds
        .recv_timeout(crate::test_support::WAIT)
        .expect("the check finished");
    assert_eq!(
        outcomes(&report),
        [
            (Stream::Availability, StreamOutcome::Idle),
            (Stream::Token, StreamOutcome::Paused),
        ]
    );
    assert!(matches!(
        companion(),
        Err(DatabaseError::OutcomeUnknown { .. })
    ));
    assert_paused_without_attempts(&rig, &handle);

    // Recovery reconciles what really committed; the service retries nothing.
    let recovery = wait(handle.recover_storage()).expect("recover storage");
    assert_eq!(
        recovery.reconciled.len(),
        2,
        "the batch and the companion write were uncertain"
    );
    let batch_committed = recovery.reconciled.iter().any(|(_, status)| {
        matches!(status, OperationStatus::Committed(marker) if marker.kind == crate::persistence::OperationKind::ConsumeTokenUsage)
    });
    assert_eq!(
        token_progress(&handle).consumed_through.is_some(),
        batch_committed
    );
    assert!(is_started_only(&job_record(&handle, service_job)));

    // The still-live service rereads the committed state and finishes the work.
    for _ in 0..2 {
        rig.tick();
    }
    assert_eq!(rig.service.state(), ServiceState::Running);
    assert_eq!(
        token_bucket(&handle, provider, "model-a", micros).map(|totals| totals.requests),
        Some(1),
        "counted once whether or not the uncertain batch had committed"
    );
    assert_eq!(
        token_progress(&handle).consumed_through,
        job_metrics(&handle, job).last().map(|record| record.id),
        "no raw fact was skipped"
    );
    assert_matches_reducer(&handle, &[job]);
    let availability =
        wait(handle.job_availability(job, None, 10)).expect("read AvailabilityStats");
    assert_eq!(
        wait(handle.availability_progress())
            .expect("progress")
            .consumed_through,
        availability.last().map(|record| record.id)
    );
    wait(rig.service.shutdown())
        .into_result()
        .expect("a clean stop");
    worker.shutdown().expect("shutdown");
}

#[test]
fn a_host_run_keeps_stopping_after_a_failed_step_and_reports_every_failure() {
    use crate::host::{Phase, run_reporting};
    let (_root, directory) = data_directory("host-cleanup");
    // The host opens the database with its system policy, so seed the same way.
    let policy = crate::system_policy::SystemPolicy::load_for_startup(&directory).expect("policy");
    let worker = DatabaseWorker::start_with_policy(&directory, policy).expect("start database");
    let handle = worker.handle();
    let (_provider, llm) = llm_setup(&handle, "model-a");
    let job = llm_job(&handle, llm);
    worker.shutdown().expect("shutdown");

    let report = run_reporting(&directory, false, move |database| async move {
        // A Token fact arrives and the commit that consumes it really fails,
        // which latches storage under the running host.
        database
            .record_metrics(NewMetrics {
                job,
                request: RequestNumber::FIRST,
                observed_at: at(at_minute(35)),
                measurements: TokenMeasurements::default(),
                token: Some(ELIGIBLE),
            })
            .await
            .expect("record Metrics");
        let mut plan = database
            .prepare_token(10)
            .await
            .expect("prepare")
            .expect("no contradiction");
        plan.inject_failure = true;
        let error = database
            .commit_token(plan)
            .await
            .expect_err("the commit fails");
        assert!(matches!(error, DatabaseError::RolledBack(_)), "{error:?}");
    });

    assert_eq!(
        report.attempted,
        [
            Phase::AggregationService,
            Phase::LogCleanup,
            Phase::ExecutionRuntime,
            Phase::Logger,
            Phase::Database,
        ],
        "every stop step ran, in order, database last"
    );
    let phases: Vec<Phase> = report
        .failures
        .iter()
        .map(|failure| failure.phase)
        .collect();
    assert!(phases.contains(&Phase::AggregationService), "{phases:?}");
    assert!(phases.contains(&Phase::Database), "{phases:?}");
    let error = report.into_result().expect_err("the run failed");
    let text = error.to_string();
    assert!(
        text.contains("aggregation service") && text.contains("database"),
        "one report carries every component's failure: {text}"
    );
}

// ---- local Control Plane ------------------------------------------------------

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
        let report =
            run_controlled_reporting(directory, false, control, move |context: HostContext| {
                let address = context.control_plane();
                async move {
                    let value = std::thread::spawn(move || client(address))
                        .join()
                        .expect("the client did not panic");
                    *slot.lock().expect("lock") = Some(value);
                }
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
            wait(handle.start_job(running)).expect("start");
            let mut settled = Vec::new();
            for finalization in [
                JobFinalization::Completed(result("done")),
                JobFinalization::Failed(result("failed")),
                JobFinalization::Cancelled(result("cancelled")),
            ] {
                let job = llm_job(handle, llm);
                wait(handle.start_job(job)).expect("start");
                wait(handle.finalize_job(job, finalization)).expect("settle");
                settled.push(job);
            }
            // Unknown, its local execution confirmed stopped.
            let confirmed = llm_job(handle, llm);
            wait(handle.start_local_job(confirmed)).expect("start");
            progress(handle, confirmed, "partial answer");
            wait(handle.finalize_job(confirmed, JobFinalization::Unknown)).expect("unknown");
            wait(handle.confirm_job_stopped(confirmed)).expect("confirm");
            // Unknown, its stop never confirmed.
            let unconfirmed = llm_job(handle, llm);
            wait(handle.start_job(unconfirmed)).expect("start");
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
                    "database": {"status": "open", "accepting_commands": true, "readable": true},
                })
            );
            let states = [
                "unsettled",
                "unsettled",
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
                let settled_at = [
                    record.completed_at,
                    record.failed_at,
                    record.cancelled_at,
                    record.unknown_at,
                ]
                .into_iter()
                .flatten()
                .next()
                .map(|at| at.as_unix_micros());
                assert_eq!(job["settlement"]["settled_at_us"], json!(settled_at));
                assert_eq!(
                    job["started_at_us"],
                    json!(record.started_at.map(|at| at.as_unix_micros()))
                );
                assert_eq!(
                    job["stop_confirmed_at_us"],
                    json!(record.stop_confirmed_at.map(|at| at.as_unix_micros()))
                );
                assert_eq!(job["created_at_us"], record.created_at.as_unix_micros());
            }
            // The Unknown Job's stop is confirmed or not, as recorded, and
            // it has no final Result: only the progress it recorded.
            let confirmed = &expected[5];
            let unconfirmed = &expected[6];
            assert!(
                confirmed.stop_confirmed_at.is_some() && unconfirmed.stop_confirmed_at.is_none()
            );
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
        for (before, after) in jobs.iter().zip(after.iter()) {
            assert_eq!(before.updated_at, after.updated_at);
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
            wait(handle.start_job(job)).expect("start");
            for index in 0..7 {
                progress(handle, job, &format!("chunk {index} \"quoted\" ünïcødé\n"));
            }
            wait(handle.finalize_job(job, JobFinalization::Completed(result("final answer"))))
                .expect("complete");
            let empty = llm_job(handle, llm);
            let large = llm_job(handle, llm);
            wait(handle.start_job(large)).expect("start");
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

    use super::*;
    use crate::disk_protection::{
        CleanupConfig, FilesystemConfig, ProtectedArea, ProtectionReason, SpaceShortage,
    };
    use crate::orchestration::execution_runtime::{ProviderExecutionPlan, confirm_stopped};
    use crate::persistence::DependencyCondition::{Settled, Success};
    use crate::persistence::DependencyState::{Impossible, Waiting};
    use crate::persistence::{OperationKind, OperationMarker};
    use crate::provider_execution::{
        CancellationToken, EnvironmentCredentialResolver, ProviderExecutionConfig,
    };
    use crate::storage::StorageError;

    fn record(handle: &DatabaseHandle, job: JobId) -> JobRecord {
        wait(handle.job(job))
            .expect("read job")
            .expect("job exists")
    }

    /// A configured Job that this process started as its own execution.
    fn local_child(handle: &DatabaseHandle, agent: ProfileId, llm: ProfileId) -> JobId {
        let job = configured_child(handle, agent, llm);
        wait(handle.start_local_job(job)).expect("start the Job locally");
        job
    }

    fn reconciliations(handle: &DatabaseHandle) -> Vec<OperationMarker> {
        wait(handle.operation_markers(None, 1_000))
            .expect("read markers")
            .into_iter()
            .filter(|marker| marker.kind == OperationKind::QuarantineAbandonedJob)
            .collect()
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
        let executor = executor(&handle, "llm worker", ExecutorType::Llm, &[OPENAI])
            .expect("register Executor");
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
        for profile in [agent, targeted] {
            assert_eq!(
                blocking(&handle, profile),
                [active],
                "only work that is still running holds its Profiles"
            );
        }
        for job in settled {
            wait(confirm_stopped(&handle, job)).expect("confirming again changes nothing");
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
        let executor = executor(&handle, "llm worker", ExecutorType::Llm, &[OPENAI])
            .expect("register Executor");
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
        assert_eq!(blocking(&handle, targeted), [job]);
        worker.shutdown().expect("shutdown");
    }

    #[test]
    fn work_abandoned_by_a_closed_opening_is_quarantined_unknown_and_unstopped_exactly_once() {
        let (_root, directory) = data_directory("abandoned");
        let worker = DatabaseWorker::start(&directory).expect("start database");
        let handle = worker.handle();
        let Configured {
            targeted, agent, ..
        } = configured(&handle);
        assert!(handle.abandoned_at_open().is_empty());
        assert_eq!(
            violation(wait(handle.start_local_job(handle.genesis()))),
            Violation::GenesisProtected(handle.genesis()),
            "the Genesis Job is never executed"
        );

        // A queued parent whose locally started child is left running.
        let parent = create_job(&handle, new_job("parent", handle.genesis(), Vec::new()));
        let child = create_job(
            &handle,
            NewJob {
                configuration: configuration(agent, targeted),
                ..new_job("running child", parent, Vec::new())
            },
        );
        wait(handle.start_local_job(child)).expect("start the child locally");
        // Locally started work whose outcome was recorded unknown, unconfirmed.
        let unconfirmed = local_child(&handle, agent, targeted);
        wait(handle.finalize_job(unconfirmed, JobFinalization::Unknown)).expect("unknown");
        let unknown_at = record(&handle, unconfirmed).unknown_at;
        // Locally started work that finished.
        let finished = local_child(&handle, agent, targeted);
        wait(handle.finalize_job(finished, JobFinalization::Completed(result("done"))))
            .expect("finish");
        // Work started outside this process is never presumed stopped.
        let external = configured_child(&handle, agent, targeted);
        wait(handle.start_job(external)).expect("start externally");
        let finished_before = record(&handle, finished);
        // The opening closes with its work unsettled, as when the process dies.
        worker.shutdown().expect("shutdown");

        let worker = DatabaseWorker::start(&directory).expect("restart");
        let handle = worker.handle();
        let mut abandoned = handle.abandoned_at_open().to_vec();
        abandoned.sort();
        assert_eq!(abandoned, [child, unconfirmed]);
        let child_record = record(&handle, child);
        // The opening that started it is closed, which makes its outcome
        // unknown. Its stop is a separate question, and nothing answered it:
        // the supervisor's death is not its descendants' termination.
        assert!(child_record.unknown_at.is_some() && child_record.stop_confirmed_at.is_none());
        assert_eq!(
            (
                child_record.completed_at,
                child_record.failed_at,
                child_record.cancelled_at,
                child_record.sterilized_at
            ),
            (None, None, None, None),
            "an abandoned Job is not given a result"
        );
        assert!(
            wait(handle.job_outputs(child, None, 10))
                .expect("read outputs")
                .is_empty()
        );
        let unconfirmed_record = record(&handle, unconfirmed);
        assert_eq!(
            unconfirmed_record.unknown_at, unknown_at,
            "the recorded outcome is kept"
        );
        assert!(unconfirmed_record.stop_confirmed_at.is_none());
        assert_eq!(record(&handle, finished), finished_before);
        let external_record = record(&handle, external);
        assert!(
            external_record.started_at.is_some()
                && external_record.unknown_at.is_none()
                && external_record.stop_confirmed_at.is_none()
        );
        for profile in [agent, targeted] {
            let mut blocked = blocking(&handle, profile);
            blocked.sort();
            // The quarantined Jobs keep holding the Profiles they were
            // submitted with: their claims outlive the opening that took them.
            assert_eq!(blocked, [child, unconfirmed, external]);
        }
        // The abandoned child's external work is not known to have stopped, so
        // its subtree is not settled and the parent stays open. Settling it
        // would be exactly the unproven release this contract forbids.
        assert_eq!(
            violation(wait(
                handle.finalize_job(parent, JobFinalization::Completed(result("done")))
            )),
            Violation::UnfinishedChild(parent),
        );
        let markers = reconciliations(&handle);
        assert_eq!(markers.len(), 2);
        assert!(
            markers
                .iter()
                .all(|marker| marker.operation.epoch() == handle.epoch())
        );
        let child_after = record(&handle, child);
        let quarantined = wait(handle.unresolved_executions()).expect("read quarantine");
        let mut quarantined_jobs: Vec<JobId> = quarantined.iter().map(|(job, _)| *job).collect();
        quarantined_jobs.sort();
        assert_eq!(quarantined_jobs, [child, unconfirmed]);
        assert!(
            quarantined.iter().all(|(_, record)| record.pool.is_none()),
            "neither Job was Pool-claimed, so only the global domain is charged"
        );
        worker.shutdown().expect("shutdown");

        let worker = DatabaseWorker::start(&directory).expect("restart again");
        let handle = worker.handle();
        assert!(
            handle.abandoned_at_open().is_empty(),
            "quarantined work is not quarantined twice"
        );
        assert_eq!(record(&handle, child), child_after);
        assert_eq!(
            wait(handle.unresolved_executions())
                .expect("read quarantine")
                .len(),
            2,
            "the protection survives a further restart unchanged"
        );
        assert_eq!(reconciliations(&handle).len(), 2);
        worker.shutdown().expect("shutdown");
        assert_relationships_consistent(&directory);
    }

    #[test]
    fn a_reopen_during_recovery_never_settles_its_own_process_work() {
        let (_root, directory) = data_directory("recovery-reopen");
        let worker = DatabaseWorker::start_with_config(&directory, lenient()).expect("start");
        let handle = worker.handle();
        let Configured {
            targeted, agent, ..
        } = configured(&handle);
        let running = local_child(&handle, agent, targeted);
        let epoch = handle.epoch();

        assert!(
            handle.activate_filesystem_protection(ProtectionReason::StorageUnavailable(
                StorageError::corrupted("injected storage failure")
            ))
        );
        let report = wait(handle.recover_storage()).expect("recover");
        assert!(
            report
                .reopened_epoch
                .is_some_and(|reopened| reopened > epoch)
        );
        let running_record = record(&handle, running);
        assert!(running_record.unknown_at.is_none() && running_record.stop_confirmed_at.is_none());
        assert_eq!(blocking(&handle, targeted), [running]);
        assert!(reconciliations(&handle).is_empty());
        wait(handle.finalize_job(running, JobFinalization::Completed(result("done"))))
            .expect("the running work settles normally");
        worker.shutdown().expect("shutdown");

        let worker = DatabaseWorker::start(&directory).expect("restart");
        assert!(worker.handle().abandoned_at_open().is_empty());
        worker.shutdown().expect("shutdown");
    }

    #[test]
    fn an_abandoned_aggregation_service_job_is_quarantined_by_the_next_start() {
        let (_root, directory) = data_directory("abandoned-service");
        let worker = DatabaseWorker::start(&directory).expect("start database");
        let handle = worker.handle();
        let service = wait(AggregationService::start(
            handle.clone(),
            AggregationServiceConfig {
                interval: Duration::from_secs(3_600),
                check_immediately: false,
                seams: TestSeams::default(),
            },
        ))
        .expect("start the service");
        let job = service.job();
        // Dropped without a shutdown, as a dying process leaves it.
        drop(service);
        assert!(record(&handle, job).started_at.is_some());
        worker.shutdown().expect("shutdown");

        let worker = DatabaseWorker::start(&directory).expect("restart");
        let handle = worker.handle();
        assert_eq!(handle.abandoned_at_open(), [job]);
        let job_record = record(&handle, job);
        // Unknown, because the opening that started it is gone. Not confirmed
        // stopped, because that opening's death says nothing about the external
        // work it may have left running.
        assert!(job_record.unknown_at.is_some() && job_record.stop_confirmed_at.is_none());
        assert!(job_record.completed_at.is_none() && job_record.failed_at.is_none());

        // A service that ends normally leaves nothing to settle.
        let service = wait(AggregationService::start(
            handle.clone(),
            AggregationServiceConfig {
                interval: Duration::from_secs(3_600),
                check_immediately: false,
                seams: TestSeams::default(),
            },
        ))
        .expect("start the next service");
        wait(service.shutdown())
            .into_result()
            .expect("end the service");
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
        let provider = wait(handle.create_llm_provider(NewLlmProvider {
            name: "silent".into(),
            transport: OPENAI,
            endpoint: Some(endpoint),
            credential_ref: None,
        }))
        .expect("create provider");
        let llm = llm_profile(
            handle,
            provider,
            llm_config("model-s", &[ModelCapability::Chat]),
        );
        let executor = executor(handle, "llm worker", ExecutorType::Llm, &[OPENAI])
            .expect("register Executor");
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
                ..new_job("provider request", handle.genesis(), Vec::new())
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
        runtime.shutdown().expect("shutdown runtime");
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
                ..new_job("plain plan", handle.genesis(), Vec::new())
            },
            outcome: JobOutcome::Completed,
            result: result("answered"),
        }));

        let started = Instant::now();
        runtime.shutdown().expect("shutdown runtime");
        assert!(
            started.elapsed() < Duration::from_secs(5),
            "shutdown waited for the request's timeout"
        );
        for refused in [queued_provider(), queued_plain()] {
            assert!(
                matches!(refused, Err(ExecutionRuntimeError::ShuttingDown)),
                "a queued plan is refused, got {refused:?}"
            );
        }
        assert_eq!(
            job_count(&handle),
            jobs_running,
            "refused plans persisted nothing"
        );
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

    fn submitted(handle: &DatabaseHandle, input: crate::persistence::JobInputId) -> bool {
        wait(handle.job_input(input))
            .expect("read input")
            .expect("input exists")
            .submitted_at
            .is_some()
    }

    /// The kinds of the committed operations that addressed or created `job`.
    fn operations_on(handle: &DatabaseHandle, job: JobId) -> Vec<OperationKind> {
        wait(handle.operation_markers(None, 1_000))
            .expect("read markers")
            .into_iter()
            .filter(|marker| {
                marker.subject == RecordRef::Job(job) || marker.created == Some(RecordRef::Job(job))
            })
            .map(|marker| marker.kind)
            .collect()
    }

    #[test]
    fn a_local_job_is_created_and_claimed_in_one_transaction_or_not_at_all() {
        let (_root, directory) = data_directory("create-and-claim");
        let worker = DatabaseWorker::start_with_config(&directory, lenient()).expect("start");
        let handle = worker.handle();
        let Configured {
            targeted, agent, ..
        } = configured(&handle);
        let no_adapter =
            executor(&handle, "no adapter", ExecutorType::Llm, &[]).expect("register Executor");
        let parent = create_job(&handle, new_job("parent", handle.genesis(), Vec::new()));
        let child = || NewJob {
            configuration: configuration(agent, targeted),
            ..new_job("local child", parent, Vec::new())
        };
        let input = wait(handle.save_job_input(
            None,
            JobInputDraft {
                objective: Some("answer".into()),
                ..JobInputDraft::default()
            },
        ))
        .expect("save input");
        let jobs_before = job_count(&handle);
        let nothing_created = |handle: &DatabaseHandle, case: &str| {
            assert_eq!(job_count(handle), jobs_before, "{case}: no Job exists");
            assert!(
                !submitted(handle, input),
                "{case}: the input stays unsubmitted"
            );
            for profile in [agent, targeted] {
                assert!(
                    blocking(handle, profile).is_empty(),
                    "{case}: no Profile is held"
                );
            }
        };

        // Refused inside the transaction, by the creation's own rules.
        assert_eq!(
            violation(wait(handle.create_local_job_with_input(
                NewJob {
                    assigned_executor: Some(no_adapter),
                    ..child()
                },
                input
            ))),
            Violation::ExecutorLacksTransport {
                executor: no_adapter,
                transport: OPENAI
            }
        );
        nothing_created(&handle, "a rejected creation");
        // Refused before the transaction could commit.
        assert!(handle.activate_filesystem_protection(space_only(&directory)));
        let error = wait(handle.create_local_job_with_input(child(), input))
            .expect_err("protection refuses the creation");
        assert!(
            error
                .command_outcome()
                .is_some_and(|outcome| outcome != crate::database::CommandOutcome::Unknown),
            "the creation was known not to commit: {error}"
        );
        wait(handle.recover_storage()).expect("recover");
        nothing_created(&handle, "an uncommitted creation");

        let job = wait(handle.create_local_job_with_input(child(), input))
            .expect("create and claim the Job");
        assert_eq!(
            operations_on(&handle, job),
            [OperationKind::CreateJobWithInput],
            "the creation and the start are one committed operation"
        );
        assert!(record(&handle, job).started_at.is_some());
        assert!(submitted(&handle, input));
        for profile in [agent, targeted] {
            assert_eq!(blocking(&handle, profile), [job]);
        }
        // The process ends before the Provider is ever called.
        worker.shutdown().expect("shutdown");

        let worker = DatabaseWorker::start(&directory).expect("restart");
        let handle = worker.handle();
        assert_eq!(
            handle.abandoned_at_open(),
            [job],
            "the claim committed with the Job"
        );
        let job_record = record(&handle, job);
        assert!(job_record.unknown_at.is_some() && job_record.stop_confirmed_at.is_none());
        assert_eq!(
            (
                job_record.completed_at,
                job_record.failed_at,
                job_record.cancelled_at
            ),
            (None, None, None)
        );
        for profile in [agent, targeted] {
            assert_eq!(
                blocking(&handle, profile),
                [job],
                "the Profiles are still held by work whose stop is unconfirmed"
            );
        }
        assert_eq!(
            violation(wait(
                handle.finalize_job(parent, JobFinalization::Completed(result("done")))
            )),
            Violation::UnfinishedChild(parent),
            "the parent stays open while its child's execution may still run"
        );
        worker.shutdown().expect("shutdown");

        let worker = DatabaseWorker::start(&directory).expect("restart again");
        assert!(worker.handle().abandoned_at_open().is_empty());
        assert_eq!(
            wait(worker.handle().unresolved_executions())
                .expect("read quarantine")
                .len(),
            1,
            "the quarantine is not rebuilt on a further restart, nor lost"
        );
        worker.shutdown().expect("shutdown");
        assert_relationships_consistent(&directory);
    }

    #[test]
    fn the_runtime_never_commits_a_job_without_its_local_claim() {
        let (_root, directory) = data_directory("runtime-create-and-claim");
        let worker = DatabaseWorker::start(&directory).expect("start database");
        let handle = worker.handle();
        let (endpoint, _accepted) = silent_provider(1);
        let setup = silent_setup(&handle, endpoint);
        let runtime = ExecutionRuntime::start(handle.clone()).expect("start runtime");
        let plain = wait(runtime.scheduler().schedule(ExecutionPlan {
            executor_id: setup.executor,
            input: JobInputDraft::default(),
            job: NewJob {
                configuration: llm_only(setup.llm),
                ..new_job("plain plan", handle.genesis(), Vec::new())
            },
            outcome: JobOutcome::Completed,
            result: result("answered"),
        }))
        .expect("run the plan");
        let provider = match wait(runtime.scheduler().schedule_provider(provider_plan(
            &handle,
            &setup,
            Duration::from_millis(300),
        ))) {
            Ok(receipt) => receipt,
            Err(error) => panic!("the request must settle, got {error}"),
        };
        runtime.shutdown().expect("shutdown runtime");
        for job in [plain.job, provider.job] {
            let operations = operations_on(&handle, job);
            assert_eq!(operations.first(), Some(&OperationKind::CreateJobWithInput));
            assert!(
                !operations.contains(&OperationKind::StartLocalJob),
                "the start was not a later, separate commit: {operations:?}"
            );
            assert!(record(&handle, job).started_at.is_some());
        }
        assert!(blocking(&handle, setup.llm).is_empty());
        worker.shutdown().expect("shutdown");
    }

    /// A started Job settled with `finalization`.
    fn settled_job(handle: &DatabaseHandle, name: &str, finalization: JobFinalization) -> JobId {
        let job = create_job(handle, new_job(name, handle.genesis(), Vec::new()));
        wait(handle.start_job(job)).expect("start");
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
        let waiting = create_job(&handle, new_job("waiting", handle.genesis(), Vec::new()));
        let dependent = |dependencies: &[(JobId, DependencyCondition)]| {
            create_job(
                &handle,
                NewJob {
                    dependencies: dependencies.to_vec(),
                    ..new_job("dependent", handle.genesis(), Vec::new())
                },
            )
        };

        for dependencies in [
            &[(completed, Success)][..],
            &[(failed, Settled)],
            &[(completed, Success), (failed, Settled)],
        ] {
            let job = dependent(dependencies);
            wait(handle.start_job(job)).expect("a satisfied Job starts");
            let local = dependent(dependencies);
            wait(handle.start_local_job(local)).expect("a satisfied Job starts locally");
            for job in [job, local] {
                assert!(record(&handle, job).started_at.is_some());
                wait(handle.finalize_job(job, JobFinalization::Completed(result("done"))))
                    .expect("and completes");
            }
        }

        let cases = [
            (&[(waiting, Success)][..], waiting, Waiting),
            (&[(failed, Success)], failed, Impossible),
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
            assert_eq!(violation(wait(handle.start_job(job))), expected);
            assert_eq!(violation(wait(handle.start_local_job(job))), expected);
            assert_eq!(
                record(&handle, job),
                before,
                "a refused start changes nothing"
            );
            refused.push((job, state));
        }

        // A waiting Job starts once its dependency is satisfied; an
        // impossible one is closed only by an explicit failure.
        wait(handle.start_job(waiting)).expect("start the prerequisite");
        wait(handle.finalize_job(waiting, JobFinalization::Completed(result("done"))))
            .expect("complete the prerequisite");
        for (job, state) in refused {
            if state == Waiting {
                wait(handle.start_local_job(job)).expect("the waiting Job now starts");
                assert!(record(&handle, job).started_at.is_some());
                wait(handle.finalize_job(job, JobFinalization::Completed(result("done"))))
                    .expect("and completes");
            } else {
                assert!(matches!(
                    violation(wait(handle.start_local_job(job))),
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

    #[test]
    fn an_unsatisfied_dependency_rolls_back_the_whole_local_creation() {
        let (_root, directory) = data_directory("create-and-claim-gate");
        let worker = DatabaseWorker::start(&directory).expect("start database");
        let handle = worker.handle();
        let Configured {
            targeted, agent, ..
        } = configured(&handle);
        let waiting = create_job(&handle, new_job("waiting", handle.genesis(), Vec::new()));
        let failed = settled_job(&handle, "failed", JobFinalization::Failed(result("failed")));
        let child = |prerequisite| NewJob {
            configuration: configuration(agent, targeted),
            dependencies: vec![(prerequisite, Success)],
            ..new_job("local child", handle.genesis(), Vec::new())
        };
        let input =
            wait(handle.save_job_input(None, JobInputDraft::default())).expect("save input");
        let jobs_before = job_count(&handle);
        let markers = || {
            wait(handle.operation_markers(None, 1_000))
                .expect("read markers")
                .len()
        };
        let markers_before = markers();
        for (prerequisite, state) in [(waiting, Waiting), (failed, Impossible)] {
            let refused = violation(wait(
                handle.create_local_job_with_input(child(prerequisite), input),
            ));
            assert!(
                matches!(
                    refused,
                    Violation::UnsatisfiedDependency { prerequisite: named, state: found, .. }
                        if named == prerequisite && found == state
                ),
                "{refused:?}"
            );
            assert_eq!(job_count(&handle), jobs_before, "no Job survives");
            assert!(!submitted(&handle, input), "the input stays unsubmitted");
            assert_eq!(markers(), markers_before, "nothing committed");
            for profile in [agent, targeted] {
                assert!(blocking(&handle, profile).is_empty(), "no Profile is held");
            }
        }

        // The runtime refuses such a plan before anything of its Job exists.
        let compatible =
            executor(&handle, "llm worker", ExecutorType::Llm, &[OPENAI]).expect("register");
        let runtime = ExecutionRuntime::start(handle.clone()).expect("start runtime");
        match wait(runtime.scheduler().schedule(ExecutionPlan {
            executor_id: compatible,
            input: JobInputDraft::default(),
            job: child(waiting),
            outcome: JobOutcome::Completed,
            result: result("answered"),
        })) {
            Err(ExecutionRuntimeError::Database(DatabaseError::Rejected(
                Violation::UnsatisfiedDependency { state: Waiting, .. },
            ))) => {}
            other => panic!("a waiting plan must not start, got {other:?}"),
        }
        runtime.shutdown().expect("shutdown runtime");
        assert_eq!(job_count(&handle), jobs_before);
        worker.shutdown().expect("shutdown");

        let worker = DatabaseWorker::start(&directory).expect("restart");
        assert!(worker.handle().abandoned_at_open().is_empty());
        worker.shutdown().expect("shutdown");
        assert_relationships_consistent(&directory);
    }

    fn assert_no_outcome(handle: &DatabaseHandle, job: JobId, case: &str) {
        let job = record(handle, job);
        assert_eq!(
            (
                job.completed_at,
                job.failed_at,
                job.cancelled_at,
                job.unknown_at
            ),
            (None, None, None, None),
            "{case}: no outcome is claimed"
        );
    }

    #[test]
    fn a_completion_that_cannot_be_settled_is_reported_and_never_turned_into_failure() {
        let (_root, directory) = data_directory("unsettled-completion");
        let worker = DatabaseWorker::start_with_config(&directory, lenient()).expect("start");
        let handle = worker.handle();
        let (_provider, llm) = llm_setup(&handle, "model-a");

        // An unsettled child refuses every confirmed outcome.
        let run = token_run(&handle, llm);
        create_job(&handle, new_job("child", run.job, Vec::new()));
        match drive(&handle, run, answered(), BOUND) {
            Err(ExecutionRuntimeError::NotSettled {
                source: DatabaseError::Rejected(Violation::UnfinishedChild(job)),
                observations: None,
            }) => assert_eq!(job, run.job),
            other => panic!("an unfinished child keeps the Job unsettled, got {other:?}"),
        }
        assert_no_outcome(&handle, run.job, "an unfinished child");
        assert_answer_recorded(&handle, run.job, "an unfinished child");

        // A refusal by disk protection is no dependency refusal.
        let run = token_run(&handle, llm);
        assert!(handle.activate_filesystem_protection(space_only(&directory)));
        // No text is streamed, so no refused output write stops the stream.
        let answer = vec![
            Step::Chunk(STOP_CHUNK),
            Step::Chunk(FINAL_INPUT_ONLY),
            Step::Chunk(DONE_CHUNK),
        ];
        match drive(&handle, run, answer, BOUND) {
            Err(ExecutionRuntimeError::NotSettled {
                source,
                observations,
            }) => {
                assert!(
                    !matches!(source, DatabaseError::Rejected(_)),
                    "no business rule refused it: {source:?}"
                );
                assert!(observations.is_some(), "nothing could be recorded");
            }
            other => panic!("a protected database settles nothing, got {other:?}"),
        }
        wait(handle.recover_storage()).expect("recover");
        assert_no_outcome(&handle, run.job, "a protected database");

        // The Failed settlement of a refused completion fails in its commit:
        // it is held as the first command of a group whose durable flush
        // then fails.
        let failed = settled_job(&handle, "failed", JobFinalization::Failed(result("failed")));
        let run = dependent_run(&handle, llm, failed, Success);
        let (armed, holds) = flume::bounded::<(flume::Receiver<()>, flume::Sender<()>)>(1);
        let conductor = {
            let handle = handle.clone();
            std::thread::spawn(move || {
                let (held, release) = holds
                    .recv_timeout(crate::test_support::WAIT)
                    .expect("armed");
                held.recv_timeout(crate::test_support::WAIT)
                    .expect("the completion is held");
                let (held, release_failed) = handle.group_probe().hold_next_group();
                release.send(()).expect("release the refused completion");
                held.recv_timeout(crate::test_support::WAIT)
                    .expect("the Failed settlement is held");
                let companion = admit(handle.record_metrics(NewMetrics {
                    job: failed,
                    request: RequestNumber::FIRST,
                    observed_at: at(1),
                    measurements: Measurements {
                        ttfb_ms: Some(1),
                        ..Measurements::default()
                    },
                    token: None,
                }));
                handle
                    .group_probe()
                    .fail_next_flush
                    .store(true, std::sync::atomic::Ordering::SeqCst);
                release_failed
                    .send(())
                    .expect("release the Failed settlement");
                companion()
            })
        };
        let settlement = {
            let handle = handle.clone();
            on_ntex(async move {
                let config = ProviderExecutionConfig::default();
                let mut sink = output_sink(handle.clone(), run.job);
                announce(Some(&mut sink), ProviderFact::Delivered).await;
                let execution = read_response(
                    payload(answered(), config.cancellation.clone()),
                    200,
                    Instant::now(),
                    None,
                    &config,
                    Some(&mut sink),
                    &ChatConversation::default(),
                )
                .await;
                armed
                    .send(handle.group_probe().hold_next_group())
                    .expect("hand the hold over");
                finish_provider_job(handle, run.executor, run.input, run.job, sink, execution).await
            })
        };
        assert!(matches!(
            conductor.join().expect("the conductor finished"),
            Err(DatabaseError::OutcomeUnknown { .. })
        ));
        match settlement {
            Err(ExecutionRuntimeError::NotSettled {
                source: DatabaseError::OutcomeUnknown { .. },
                observations,
            }) => assert!(observations.is_some(), "nothing more could be recorded"),
            other => panic!("the failed settlement must be reported, got {other:?}"),
        }
        wait(handle.recover_storage()).expect("recover");
        let job = record(&handle, run.job);
        assert!(
            job.completed_at.is_none() && job.unknown_at.is_none() && job.cancelled_at.is_none(),
            "no other outcome is made up"
        );
        assert!(
            wait(handle.job_availability(run.job, None, 10))
                .expect("read AvailabilityStats")
                .iter()
                .all(|stat| stat.observation.failure.is_none()),
            "no Provider failure is recorded"
        );
        worker.shutdown().expect("shutdown");
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

        for (first, committed) in [
            (eligible(), RequestEligibility::Eligible),
            (excluded(), RequestEligibility::Excluded),
        ] {
            let job = llm_job(&handle, llm);
            // Generic Metrics carry no eligibility and never conflict.
            wait(handle.record_metrics(fact(job, 1, None))).expect("generic Metrics");
            wait(handle.record_metrics(fact(job, 2, first))).expect("the first annotation");
            wait(handle.record_metrics(fact(job, 3, first))).expect("a consistent repeat");
            wait(handle.record_metrics(fact(job, 4, None))).expect("generic Metrics after");

            let records = job_metrics(&handle, job);
            let markers = wait(handle.operation_markers(None, 1_000))
                .expect("read markers")
                .len();
            let progress = token_progress(&handle);
            let contradicting = match committed {
                RequestEligibility::Eligible => excluded(),
                RequestEligibility::Excluded => eligible(),
            };
            assert_eq!(
                violation(wait(handle.record_metrics(fact(job, 5, contradicting)))),
                contradiction(job, committed)
            );
            assert_eq!(job_metrics(&handle, job), records, "nothing was written");
            assert_eq!(
                wait(handle.operation_markers(None, 1_000))
                    .expect("read markers")
                    .len(),
                markers,
                "no marker, so nothing committed"
            );
            assert_eq!(token_progress(&handle), progress);
        }

        // Aggregation goes on: every Job's evidence reduces.
        let consumed = wait(handle.consume_token_usage(1_000)).expect("aggregate");
        assert!(matches!(consumed, TokenBatch::Consumed(_)), "{consumed:?}");
        assert!(matches!(
            wait(handle.consume_token_usage(1_000)).expect("aggregate"),
            TokenBatch::Idle { .. }
        ));
        worker.shutdown().expect("shutdown");

        // The check holds for evidence committed by an earlier opening.
        let worker = DatabaseWorker::start(&directory).expect("reopen");
        let handle = worker.handle();
        let job = wait(handle.jobs(None, 1_000))
            .expect("read jobs")
            .into_iter()
            .find(|job| {
                job_metrics(&handle, job.id)
                    .iter()
                    .any(|record| record.token == excluded())
            })
            .expect("the excluded Job")
            .id;
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
        let stored: Vec<_> = job_metrics(&handle, job)
            .into_iter()
            .map(|record| record.token)
            .collect();
        assert_eq!(stored.len(), accepted.len());
        assert!(stored.iter().all(|token| *token == accepted[0]));
        let consumed = wait(handle.consume_token_usage(1_000)).expect("aggregate");
        assert!(matches!(consumed, TokenBatch::Consumed(_)), "{consumed:?}");
        worker.shutdown().expect("shutdown");
    }
}

// ---- aggregation liveness ----------------------------------------------------------

/// A loop that exits is never reported running, a known rollback is retried
/// from committed progress, and an unknown commit is resolved, not replayed.
mod aggregation_liveness {
    use super::*;
    use crate::database::{StorageError, UnknownCause};
    use crate::persistence::OperationId;

    const PANIC: &str = "injected aggregation fault";

    #[test]
    fn a_panicking_loop_is_reported_failed_with_its_message_and_ends_the_run() {
        let (_root, directory) = data_directory("liveness-panic");
        let worker = DatabaseWorker::start(&directory).expect("start database");
        let handle = worker.handle();
        let (_provider, llm) = llm_setup(&handle, "model-a");
        let job = llm_job(&handle, llm);
        let panic_now = Arc::new(AtomicBool::new(false));
        let armed = panic_now.clone();
        let rig = Rig::start(&handle, move |seams| {
            seams.before_consume = Some(Arc::new(move |_| {
                if armed.load(AtomicOrdering::SeqCst) {
                    panic!("{PANIC}");
                }
            }));
        });
        rig.tick();
        assert_eq!(rig.service.state(), ServiceState::Running);

        observe(&handle, job, at_minute(40), response(200));
        let availability = wait(handle.availability_progress()).expect("progress");
        panic_now.store(true, AtomicOrdering::SeqCst);
        assert!(rig.service.tick());
        eventually(|| rig.service.state() != ServiceState::Running);
        match rig.service.state() {
            ServiceState::Failed(reason) => {
                assert!(
                    reason.contains("panicked") && reason.contains(PANIC),
                    "{reason}"
                )
            }
            other => panic!("a panicked loop is Failed, got {other:?}"),
        }
        eventually(|| !rig.service.tick());
        assert!(matches!(rig.service.state(), ServiceState::Failed(_)));
        assert_eq!(
            wait(handle.availability_progress()).expect("progress"),
            availability,
            "nothing was committed by the interrupted check"
        );

        let service = rig.service;
        let service_job = service.job();
        let outcome = wait(service.shutdown());
        assert!(matches!(outcome.failure, Some(ServiceFailure::Panicked)));
        assert!(matches!(outcome.end, JobEnd::Recorded));
        assert!(job_record(&handle, service_job).failed_at.is_some());
        worker.shutdown().expect("shutdown");
    }

    /// Makes the next consume call of `stream` meet `error` once.
    fn fail_once(
        seams: &mut TestSeams,
        stream: Stream,
        error: impl Fn() -> DatabaseError + Send + Sync + 'static,
    ) {
        let armed = AtomicBool::new(true);
        seams.error_override = Some(Arc::new(move |seen| {
            (seen == stream && armed.swap(false, AtomicOrdering::SeqCst)).then(&error)
        }));
    }

    #[test]
    fn a_rollback_on_an_open_database_is_retried_from_committed_progress() {
        let (_root, directory) = data_directory("liveness-rollback");
        let worker = DatabaseWorker::start(&directory).expect("start database");
        let handle = worker.handle();
        let (provider, llm) = llm_setup(&handle, "model-a");
        let job = llm_job(&handle, llm);
        let micros = at_minute(41);
        let fact = token_fact(
            &handle,
            job,
            micros,
            Some(ELIGIBLE_FINAL),
            io5(Some(2), Some(3)),
        );
        let rig = Rig::start(&handle, |seams| {
            fail_once(seams, Stream::Token, || {
                DatabaseError::RolledBack(StorageError::corrupted("injected rollback"))
            })
        });

        let report = rig.tick();
        assert!(
            outcomes(&report).contains(&(Stream::Token, StreamOutcome::Retry)),
            "{report:?}"
        );
        assert_eq!(rig.service.state(), ServiceState::Running);
        assert_eq!(
            token_progress(&handle).consumed_through,
            None,
            "nothing moved"
        );

        let report = rig.tick();
        assert!(
            outcomes(&report).contains(&(Stream::Token, StreamOutcome::Consumed(1))),
            "{report:?}"
        );
        assert_eq!(token_progress(&handle).consumed_through, Some(fact));
        assert_eq!(
            token_bucket(&handle, provider, "model-a", micros).map(|totals| totals.requests),
            Some(1),
            "counted once"
        );
        wait(rig.service.shutdown())
            .into_result()
            .expect("a clean stop");
        worker.shutdown().expect("shutdown");
    }

    #[test]
    fn an_unknown_commit_on_an_open_database_is_resolved_before_its_stream_runs_again() {
        let (_root, directory) = data_directory("liveness-unknown");
        let worker = recovering_worker(&directory);
        let handle = worker.handle();
        let (provider, llm) = llm_setup(&handle, "model-a");
        let job = llm_job(&handle, llm);
        let micros = at_minute(42);
        let fact = token_fact(
            &handle,
            job,
            micros,
            Some(ELIGIBLE_FINAL),
            io5(Some(2), Some(3)),
        );
        // An operation of this opening that has no marker yet.
        let uncertain = OperationId::new(handle.epoch(), u64::MAX);
        let rig = Rig::start(&handle, move |seams| {
            fail_once(seams, Stream::Token, move || {
                DatabaseError::OutcomeUnknown {
                    operation: uncertain,
                    cause: UnknownCause::Commit(StorageError::corrupted("injected unknown commit")),
                }
            })
        });

        let report = rig.tick();
        assert!(
            outcomes(&report).contains(&(Stream::Token, StreamOutcome::Uncertain)),
            "{report:?}"
        );
        // While its marker cannot say, the Token stream is not attempted;
        // Availability goes on.
        observe(&handle, job, micros, response(200));
        let calls = rig.calls();
        let mut availability = 0;
        for _ in 0..3 {
            for (stream, outcome) in outcomes(&rig.tick()) {
                match stream {
                    Stream::Token => assert_eq!(outcome, StreamOutcome::Uncertain),
                    Stream::Availability => {
                        if let StreamOutcome::Consumed(records) = outcome {
                            availability += records;
                        }
                    }
                }
            }
        }
        assert_eq!(availability, 1);
        assert_eq!(rig.calls(), calls + 3, "only Availability was attempted");
        assert_eq!(
            token_progress(&handle).consumed_through,
            None,
            "the cursor did not move on an unconfirmed commit"
        );

        // A reopen settles that the operation never committed; the stream
        // then prepares again from what is committed.
        assert!(handle.activate_filesystem_protection(
            crate::disk_protection::ProtectionReason::StorageUnavailable(StorageError::corrupted(
                "injected storage failure"
            ))
        ));
        wait(handle.recover_storage()).expect("recover storage");
        let report = rig.tick();
        assert!(
            outcomes(&report).contains(&(Stream::Token, StreamOutcome::Consumed(1))),
            "{report:?}"
        );
        assert_eq!(token_progress(&handle).consumed_through, Some(fact));
        assert_eq!(
            token_bucket(&handle, provider, "model-a", micros).map(|totals| totals.requests),
            Some(1),
            "counted once"
        );
        assert_eq!(rig.service.state(), ServiceState::Running);
        wait(rig.service.shutdown())
            .into_result()
            .expect("a clean stop");
        worker.shutdown().expect("shutdown");
    }
}

// ---- settlement, dependencies and retries --------------------------------------

mod settlement {
    use std::sync::{Arc, Barrier};

    use super::*;
    use crate::persistence::{JobInputId, OperationKind};
    use crate::storage::{JobRow, dependency_state};
    use DependencyCondition::{Settled, Success};
    use DependencyState::{Impossible, Satisfied, Waiting};

    /// Brings a new Job into the state one table row names.
    type Settle<'a> = &'a dyn Fn(JobId);

    fn record(handle: &DatabaseHandle, job: JobId) -> JobRecord {
        wait(handle.job(job))
            .expect("read job")
            .expect("job exists")
    }

    fn job(handle: &DatabaseHandle, name: &str) -> JobId {
        create_job(handle, new_job(name, handle.genesis(), Vec::new()))
    }

    fn finalize(handle: &DatabaseHandle, job: JobId, finalization: JobFinalization) {
        wait(handle.finalize_job(job, finalization)).expect("finalize");
    }

    /// Records an unknown outcome, without confirming any stop.
    fn unknown(handle: &DatabaseHandle, job: JobId) {
        finalize(handle, job, JobFinalization::Unknown);
    }

    fn stopped(handle: &DatabaseHandle, job: JobId) {
        wait(handle.confirm_job_stopped(job)).expect("the trusted path confirms the stop");
    }

    /// A Job whose explicit prerequisite is `prerequisite`.
    fn dependent(
        handle: &DatabaseHandle,
        prerequisite: JobId,
        condition: DependencyCondition,
    ) -> JobId {
        create_job(
            handle,
            NewJob {
                dependencies: vec![(prerequisite, condition)],
                ..new_job("dependent", handle.genesis(), Vec::new())
            },
        )
    }

    /// The state a Completed finalization finds the dependency in. A
    /// satisfied dependency lets the Job complete; any other refuses it,
    /// and then the Job still fails, so the probe leaves it settled.
    fn completion_probe(
        handle: &DatabaseHandle,
        job: JobId,
        prerequisite: JobId,
    ) -> DependencyState {
        match wait(handle.finalize_job(job, JobFinalization::Completed(result("done")))) {
            Ok(_) => Satisfied,
            Err(DatabaseError::Rejected(Violation::UnsatisfiedDependency {
                job: refused,
                prerequisite: named,
                state,
            })) => {
                assert_eq!((refused, named), (job, prerequisite));
                assert_ne!(state, Satisfied);
                assert!(
                    record(handle, job).completed_at.is_none(),
                    "a refused completion changes nothing"
                );
                finalize(
                    handle,
                    job,
                    JobFinalization::Failed(result("prerequisite unmet")),
                );
                state
            }
            other => panic!("expected a completion or its refusal, got {other:?}"),
        }
    }

    #[test]
    fn the_dependency_table_follows_settlement() {
        let at = Some(Timestamp::from_unix_micros(1));
        let queued = JobRow {
            id: JobId::new(1),
            name: None,
            execution_mode: ExecutionMode::Once,
            eligible_at: None,
            started_at: None,
            paused_at: None,
            sterilized_at: None,
            completed_at: None,
            failed_at: None,
            cancelled_at: None,
            unknown_at: None,
            stop_confirmed_at: None,
            next_due_at: None,
            next_iteration_no: None,
            iteration_no: None,
            scheduled_at: None,
            created_at: Timestamp::from_unix_micros(0),
            updated_at: Timestamp::from_unix_micros(0),
        };
        let started = JobRow {
            started_at: at,
            ..queued.clone()
        };
        let rows = [
            (
                "completed",
                JobRow {
                    completed_at: at,
                    ..started.clone()
                },
                Satisfied,
                Satisfied,
            ),
            (
                "failed",
                JobRow {
                    failed_at: at,
                    ..started.clone()
                },
                Impossible,
                Satisfied,
            ),
            (
                "cancelled",
                JobRow {
                    cancelled_at: at,
                    ..started.clone()
                },
                Impossible,
                Satisfied,
            ),
            (
                "unknown, stop confirmed",
                JobRow {
                    unknown_at: at,
                    stop_confirmed_at: at,
                    ..started.clone()
                },
                Impossible,
                Satisfied,
            ),
            (
                "unknown, stop unconfirmed",
                JobRow {
                    unknown_at: at,
                    ..started.clone()
                },
                Waiting,
                Waiting,
            ),
            ("queued", queued.clone(), Waiting, Waiting),
            ("running", started.clone(), Waiting, Waiting),
            (
                "paused",
                JobRow {
                    paused_at: at,
                    ..started.clone()
                },
                Waiting,
                Waiting,
            ),
        ];
        for (case, row, success, settled) in rows {
            assert_eq!(dependency_state(Success, &row), success, "{case}: Success");
            assert_eq!(dependency_state(Settled, &row), settled, "{case}: Settled");
            assert_eq!(row.settled(), settled == Satisfied, "{case}: settled");
        }
    }

    #[test]
    fn completion_finds_every_dependency_as_the_table_states() {
        let (_root, directory) = data_directory("dependency-table");
        let worker = DatabaseWorker::start(&directory).expect("start database");
        let handle = worker.handle();
        let state = |name: &str, settle: &dyn Fn(JobId)| {
            let prerequisite = job(&handle, name);
            settle(prerequisite);
            let success = dependent(&handle, prerequisite, Success);
            let settled = dependent(&handle, prerequisite, Settled);
            (
                completion_probe(&handle, success, prerequisite),
                completion_probe(&handle, settled, prerequisite),
            )
        };
        let rows: [(&str, Settle, (DependencyState, DependencyState)); 7] = [
            (
                "completed",
                &|job| finalize(&handle, job, JobFinalization::Completed(result("done"))),
                (Satisfied, Satisfied),
            ),
            (
                "failed",
                &|job| finalize(&handle, job, JobFinalization::Failed(result("broken"))),
                (Impossible, Satisfied),
            ),
            (
                "cancelled",
                &|job| finalize(&handle, job, JobFinalization::Cancelled(result("stopped"))),
                (Impossible, Satisfied),
            ),
            (
                "unknown, stop confirmed",
                &|job| {
                    unknown(&handle, job);
                    stopped(&handle, job);
                },
                (Impossible, Satisfied),
            ),
            (
                "unknown, stop unconfirmed",
                &|job| unknown(&handle, job),
                (Waiting, Waiting),
            ),
            ("queued", &|_| {}, (Waiting, Waiting)),
            (
                "running",
                &|job| wait(handle.start_job(job)).expect("start"),
                (Waiting, Waiting),
            ),
        ];
        for (case, settle, expected) in rows {
            assert_eq!(state(case, settle), expected, "{case}: (Success, Settled)");
        }
        worker.shutdown().expect("shutdown");
        assert_relationships_consistent(&directory);
    }

    #[test]
    fn a_confirmed_stop_settles_dependencies_without_changing_the_outcome() {
        let (_root, directory) = data_directory("stop-settles");
        let worker = DatabaseWorker::start(&directory).expect("start database");
        let handle = worker.handle();
        let prerequisite = job(&handle, "prerequisite");
        wait(handle.start_job(prerequisite)).expect("start");
        wait(handle.append_job_output(
            prerequisite,
            NewJobOutput {
                kind: IntermediateOutputKind::Progress,
                summary: Some("partial".into()),
                code: None,
                goal_reached: None,
                value: None,
            },
        ))
        .expect("publish progress");
        unknown(&handle, prerequisite);
        let before = record(&handle, prerequisite);
        let outputs = wait(handle.job_outputs(prerequisite, None, 10)).expect("read outputs");
        let needs_success = dependent(&handle, prerequisite, Success);
        let needs_settled = dependent(&handle, prerequisite, Settled);
        for job in [needs_success, needs_settled] {
            assert_eq!(
                violation(wait(
                    handle.finalize_job(job, JobFinalization::Completed(result("done")))
                )),
                Violation::UnsatisfiedDependency {
                    job,
                    prerequisite,
                    state: Waiting
                },
                "an unconfirmed unknown outcome is still a wait"
            );
        }

        stopped(&handle, prerequisite);
        wait(handle.finalize_job(needs_settled, JobFinalization::Completed(result("done"))))
            .expect("a settled dependency is satisfied by the confirmed stop");
        assert_eq!(
            violation(wait(handle.finalize_job(
                needs_success,
                JobFinalization::Completed(result("done"))
            ))),
            Violation::UnsatisfiedDependency {
                job: needs_success,
                prerequisite,
                state: Impossible
            },
            "a stopped unknown outcome never satisfies a success dependency"
        );
        let after = record(&handle, prerequisite);
        assert!(after.stop_confirmed_at.is_some());
        assert_eq!(
            JobRecord {
                stop_confirmed_at: None,
                updated_at: before.updated_at,
                ..after.clone()
            },
            before,
            "only the stop confirmation changed"
        );
        worker.shutdown().expect("shutdown");

        let worker = DatabaseWorker::start(&directory).expect("reopen database");
        let handle = worker.handle();
        assert_eq!(
            record(&handle, prerequisite),
            after,
            "the unknown outcome is kept"
        );
        assert_eq!(
            wait(handle.job_outputs(prerequisite, None, 10)).expect("read outputs"),
            outputs,
            "its outputs are kept and no result is published"
        );
        assert!(record(&handle, needs_settled).completed_at.is_some());
        assert!(record(&handle, needs_success).completed_at.is_none());
        worker.shutdown().expect("shutdown");
        assert_relationships_consistent(&directory);
    }

    #[test]
    fn a_parent_closes_once_its_unknown_child_is_confirmed_stopped() {
        let (_root, directory) = data_directory("parent-closes");
        let worker = DatabaseWorker::start(&directory).expect("start database");
        let handle = worker.handle();
        let mut closed = Vec::new();
        for finalization in [
            JobFinalization::Completed(result("done with an auxiliary child")),
            JobFinalization::Failed(result("broken")),
            JobFinalization::Cancelled(result("stopped")),
        ] {
            let parent = job(&handle, "parent");
            let child = create_job(&handle, new_job("auxiliary child", parent, Vec::new()));
            wait(handle.start_job(child)).expect("start");
            unknown(&handle, child);
            assert_eq!(
                violation(wait(handle.finalize_job(parent, finalization.clone()))),
                Violation::UnfinishedChild(parent),
                "an unknown child that may still execute keeps its parent open"
            );
            stopped(&handle, child);
            let appended = wait(handle.finalize_job(parent, finalization))
                .expect("the parent closes once its child is settled");
            assert!(appended.is_some(), "the parent publishes its own result");
            let child_record = record(&handle, child);
            assert!(child_record.unknown_at.is_some() && child_record.stop_confirmed_at.is_some());
            assert_eq!(
                (
                    child_record.completed_at,
                    child_record.failed_at,
                    child_record.cancelled_at
                ),
                (None, None, None),
                "the child stays unknown"
            );
            assert!(
                wait(handle.job_outputs(child, None, 10))
                    .expect("read outputs")
                    .is_empty(),
                "no result is fabricated for the child"
            );
            closed.push((parent, child));
        }

        // A Job that requires the child's success says so with an explicit
        // Success dependency, and a stopped unknown outcome does not meet it.
        let parent = job(&handle, "parent with a required child");
        let worker_child = create_job(&handle, new_job("worker", parent, Vec::new()));
        let finisher = create_job(
            &handle,
            NewJob {
                dependencies: vec![(worker_child, Success)],
                ..new_job("finisher needs the worker", parent, Vec::new())
            },
        );
        unknown(&handle, worker_child);
        stopped(&handle, worker_child);
        assert_eq!(
            violation(wait(handle.finalize_job(
                finisher,
                JobFinalization::Completed(result("done"))
            ))),
            Violation::UnsatisfiedDependency {
                job: finisher,
                prerequisite: worker_child,
                state: Impossible
            }
        );
        finalize(
            &handle,
            finisher,
            JobFinalization::Failed(result("the worker did not succeed")),
        );
        let records: Vec<_> = closed
            .iter()
            .flat_map(|(parent, child)| [record(&handle, *parent), record(&handle, *child)])
            .collect();
        worker.shutdown().expect("shutdown");

        let worker = DatabaseWorker::start(&directory).expect("reopen database");
        let handle = worker.handle();
        assert_eq!(
            closed
                .iter()
                .flat_map(|(parent, child)| [record(&handle, *parent), record(&handle, *child)])
                .collect::<Vec<_>>(),
            records
        );
        worker.shutdown().expect("shutdown");
        assert_relationships_consistent(&directory);
    }

    #[test]
    fn completion_needs_satisfied_prerequisites_while_failure_and_cancellation_do_not() {
        let (_root, directory) = data_directory("completion-prerequisites");
        let worker = DatabaseWorker::start(&directory).expect("start database");
        let handle = worker.handle();
        let waiting = job(&handle, "still queued");
        let impossible = job(&handle, "failed");
        finalize(
            &handle,
            impossible,
            JobFinalization::Failed(result("broken")),
        );
        for (prerequisite, state) in [(waiting, Waiting), (impossible, Impossible)] {
            let parent = dependent(&handle, prerequisite, Success);
            let child = create_job(&handle, new_job("child", parent, Vec::new()));
            finalize(
                &handle,
                child,
                JobFinalization::Completed(result("child done")),
            );
            assert_eq!(
                violation(wait(
                    handle.finalize_job(parent, JobFinalization::Completed(result("done")))
                )),
                Violation::UnsatisfiedDependency {
                    job: parent,
                    prerequisite,
                    state
                }
            );
            let refused = record(&handle, parent);
            assert!(refused.completed_at.is_none() && refused.sterilized_at.is_none());
            let closing = match state {
                Waiting => JobFinalization::Cancelled(result("no longer needed")),
                _ => JobFinalization::Failed(result("prerequisite failed")),
            };
            wait(handle.finalize_job(parent, closing))
                .expect("failure and cancellation close once the children settled");
        }
        worker.shutdown().expect("shutdown");
        assert_relationships_consistent(&directory);
    }

    #[test]
    fn the_waiting_graph_drops_a_stopped_unknown_job_and_keeps_real_cycles() {
        let (_root, directory) = data_directory("waiting-stopped");
        let worker = DatabaseWorker::start(&directory).expect("start database");
        let handle = worker.handle();
        let anchor = job(&handle, "anchor");
        let parent = job(&handle, "parent");
        // The child waits for the anchor, and its parent waits for it.
        let child = create_job(
            &handle,
            NewJob {
                dependencies: vec![(anchor, Settled)],
                ..new_job("child", parent, Vec::new())
            },
        );
        // A child that waits for the anchor can no longer start; it is
        // started as a database written before that check could hold it.
        wait(handle.start_preexisting_local_job(child)).expect("start");
        unknown(&handle, child);
        // anchor -> new -> parent -> child -> anchor: the child may still be
        // executing, so the cycle is real.
        let closing = || NewJob {
            dependencies: vec![(parent, Settled)],
            ..new_job("waits for the parent", anchor, Vec::new())
        };
        let input =
            wait(handle.save_job_input(None, JobInputDraft::default())).expect("save input");
        assert!(matches!(
            violation(wait(handle.create_job_with_input(closing(), input))),
            Violation::WaitingCycle(_)
        ));
        assert!(
            wait(handle.input_jobs(input))
                .expect("read input Jobs")
                .is_empty(),
            "the refused submission left nothing"
        );

        stopped(&handle, child);
        let admitted = wait(handle.create_job_with_input(closing(), input))
            .expect("a stopped unknown child neither holds its parent nor waits");
        assert_eq!(record(&handle, admitted).parent, anchor);
        worker.shutdown().expect("shutdown");
        assert_relationships_consistent(&directory);
    }

    fn retry(
        original: JobId,
        input: RetryInput,
        configuration: Option<ExecutionConfiguration>,
    ) -> JobRetry {
        JobRetry {
            original,
            input,
            name: Some("retry".into()),
            execution_mode: ExecutionMode::Once,
            eligible_at: None,
            scheduled_at: None,
            next_due_at: None,
            next_iteration_no: None,
            iteration_no: None,
            projects: Vec::new(),
            dependencies: Vec::new(),
            configuration,
            assigned_executor: None,
            acknowledge_repeated_effects: false,
        }
    }

    fn fresh_input(handle: &DatabaseHandle) -> JobInputId {
        wait(handle.save_job_input(
            None,
            JobInputDraft {
                objective: Some("again".into()),
                ..JobInputDraft::default()
            },
        ))
        .expect("save input")
    }

    fn retry_markers(handle: &DatabaseHandle) -> usize {
        wait(handle.operation_markers(None, 1_000))
            .expect("read markers")
            .into_iter()
            .filter(|marker| marker.kind == OperationKind::RetryJob)
            .count()
    }

    /// A refused retry submitted nothing, linked nothing and marked nothing.
    fn assert_refused_retry_left_nothing(
        handle: &DatabaseHandle,
        original: JobId,
        input: JobInputId,
        markers: usize,
        retries: &[JobId],
    ) {
        let input_record = wait(handle.job_input(input))
            .expect("read input")
            .expect("input exists");
        assert!(input_record.submitted_at.is_none() && input_record.configuration.is_none());
        assert!(
            wait(handle.input_jobs(input))
                .expect("read input Jobs")
                .is_empty()
        );
        assert_eq!(
            wait(handle.retries(original)).expect("read retries"),
            retries
        );
        assert_eq!(retry_markers(handle), markers);
    }

    #[test]
    fn only_a_settled_original_can_be_retried() {
        let (_root, directory) = data_directory("retry-eligibility");
        let worker = DatabaseWorker::start(&directory).expect("start database");
        let handle = worker.handle();
        let rows: [(&str, Settle, bool); 7] = [
            ("queued", &|_| {}, false),
            (
                "running",
                &|job| wait(handle.start_job(job)).expect("start"),
                false,
            ),
            (
                "completed",
                &|job| finalize(&handle, job, JobFinalization::Completed(result("done"))),
                true,
            ),
            (
                "failed",
                &|job| finalize(&handle, job, JobFinalization::Failed(result("broken"))),
                true,
            ),
            (
                "cancelled",
                &|job| finalize(&handle, job, JobFinalization::Cancelled(result("stopped"))),
                true,
            ),
            (
                "unknown, stop unconfirmed",
                &|job| unknown(&handle, job),
                false,
            ),
            (
                "unknown, stop confirmed",
                &|job| {
                    unknown(&handle, job);
                    stopped(&handle, job);
                },
                true,
            ),
        ];
        for (case, settle, eligible) in rows {
            let original = job(&handle, case);
            settle(original);
            let before = record(&handle, original);
            let input = fresh_input(&handle);
            let markers = retry_markers(&handle);
            match wait(handle.retry_job(retry(original, RetryInput::Changed(input), None))) {
                Ok(created) => {
                    assert!(eligible, "{case}: must be refused");
                    let created = record(&handle, created);
                    assert_eq!(
                        (created.retry_of, created.input),
                        (Some(original), input),
                        "{case}"
                    );
                }
                Err(DatabaseError::Rejected(violation)) => {
                    assert!(!eligible, "{case}: must be eligible, got {violation:?}");
                    assert_eq!(
                        violation,
                        Violation::RetryOfUnsettledJob(original),
                        "{case}"
                    );
                    assert_refused_retry_left_nothing(&handle, original, input, markers, &[]);
                }
                Err(other) => panic!("{case}: {other:?}"),
            }
            assert_eq!(
                record(&handle, original),
                before,
                "{case}: the original is untouched"
            );
        }
        worker.shutdown().expect("shutdown");
        assert_relationships_consistent(&directory);
    }

    #[test]
    fn concurrent_retries_leave_at_most_one_active_direct_retry() {
        const SUBMITTERS: usize = 8;
        let (_root, directory) = data_directory("retry-concurrent");
        let worker = DatabaseWorker::start(&directory).expect("start database");
        let handle = worker.handle();
        let Configured {
            targeted, agent, ..
        } = configured(&handle);
        let original = configured_child(&handle, agent, targeted);
        wait(handle.start_job(original)).expect("start");
        wait(handle.append_job_output(
            original,
            NewJobOutput {
                kind: IntermediateOutputKind::Progress,
                summary: Some("first attempt".into()),
                code: None,
                goal_reached: None,
                value: None,
            },
        ))
        .expect("publish progress");
        finalize(
            &handle,
            original,
            JobFinalization::Failed(result("first try failed")),
        );
        let original_input = record(&handle, original).input;
        let frozen = wait(handle.job_input(original_input))
            .expect("read input")
            .expect("input exists")
            .configuration
            .expect("a frozen configuration");
        let inputs: Vec<JobInputId> = (0..SUBMITTERS).map(|_| fresh_input(&handle)).collect();
        let markers = retry_markers(&handle);

        let barrier = Arc::new(Barrier::new(SUBMITTERS));
        let submitters: Vec<_> = inputs
            .iter()
            .map(|input| {
                let (handle, barrier, input) = (handle.clone(), barrier.clone(), *input);
                std::thread::spawn(move || {
                    barrier.wait();
                    let result = wait(handle.retry_job(retry(
                        original,
                        RetryInput::Changed(input),
                        configuration(agent, targeted),
                    )));
                    (input, result)
                })
            })
            .collect();
        let results: Vec<_> = submitters
            .into_iter()
            .map(|submitter| submitter.join().expect("submitter"))
            .collect();
        let created: Vec<_> = results
            .iter()
            .filter_map(|(input, result)| result.as_ref().ok().map(|job| (*input, *job)))
            .collect();
        let [(winning_input, active)] = created.as_slice() else {
            panic!("exactly one retry is created, got {results:?}");
        };
        let active = *active;
        for (input, result) in results {
            if input == *winning_input {
                continue;
            }
            assert_eq!(
                violation(result),
                Violation::ActiveRetry {
                    original,
                    retry: active
                },
                "every other submission is refused"
            );
            assert_refused_retry_left_nothing(&handle, original, input, markers + 1, &[active]);
        }
        assert_eq!(
            wait(handle.job_input(*winning_input))
                .expect("read input")
                .expect("input exists")
                .configuration,
            Some(frozen.clone()),
            "the retry froze the same configuration"
        );

        // An unknown retry that may still execute is unsettled too.
        wait(handle.start_job(active)).expect("start");
        unknown(&handle, active);
        let blocked = fresh_input(&handle);
        assert_eq!(
            violation(wait(handle.retry_job(retry(
                original,
                RetryInput::Changed(blocked),
                configuration(agent, targeted)
            )))),
            Violation::ActiveRetry {
                original,
                retry: active
            }
        );
        assert_refused_retry_left_nothing(&handle, original, blocked, markers + 1, &[active]);
        stopped(&handle, active);
        let next = wait(handle.retry_job(retry(
            original,
            RetryInput::Unchanged,
            configuration(agent, targeted),
        )))
        .expect("once the direct retry settled, another one is allowed");
        assert_eq!(
            violation(wait(handle.retry_job(retry(
                original,
                RetryInput::Unchanged,
                configuration(agent, targeted)
            )))),
            Violation::ActiveRetry {
                original,
                retry: next
            }
        );
        let jobs = [original, active, next].map(|job| record(&handle, job));
        let outputs = wait(handle.job_outputs(original, None, 10)).expect("read outputs");
        worker.shutdown().expect("shutdown");

        let worker = DatabaseWorker::start(&directory).expect("reopen database");
        let handle = worker.handle();
        assert_eq!(
            [original, active, next].map(|job| record(&handle, job)),
            jobs
        );
        let [original_record, active_record, next_record] = &jobs;
        assert!(original_record.failed_at.is_some());
        assert!(active_record.unknown_at.is_some() && active_record.stop_confirmed_at.is_some());
        assert_eq!(
            (
                active_record.retry_of,
                next_record.retry_of,
                next_record.input
            ),
            (Some(original), Some(original), original_input)
        );
        assert_eq!(
            wait(handle.retries(original)).expect("read retries"),
            [active, next]
        );
        assert_eq!(
            wait(handle.job_outputs(original, None, 10)).expect("read outputs"),
            outputs
        );
        for input in [original_input, *winning_input] {
            assert_eq!(
                wait(handle.job_input(input))
                    .expect("read input")
                    .expect("input exists")
                    .configuration,
                Some(frozen.clone()),
                "the frozen configuration survives reopening"
            );
        }
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
    use crate::persistence::{AvailabilityBatch, ToolEffect};
    use crate::provider_execution::ProviderExecutionError;
    use crate::token_usage::TokenBatch;

    fn number(value: u32) -> RequestNumber {
        RequestNumber::new(value).expect("a request number")
    }

    fn request(job: JobId, value: u32) -> RequestRef {
        RequestRef {
            job,
            request: number(value),
        }
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

    /// Consumes both evidence streams until neither has anything left.
    fn drain(handle: &DatabaseHandle) {
        while !matches!(
            wait(handle.consume_availability(1_000)).expect("consume availability"),
            AvailabilityBatch::Idle { .. }
        ) {}
        while !matches!(
            wait(handle.consume_token_usage(1_000)).expect("consume Token usage"),
            TokenBatch::Idle { .. }
        ) {}
    }

    fn token_bucket(
        handle: &DatabaseHandle,
        provider: LlmProviderId,
        minute: i64,
    ) -> TokenMinuteTotals {
        wait(handle.token_minutes(provider, "model-a".into(), minute, 1))
            .expect("read Token minutes")
            .value
            .first()
            .map_or_else(TokenMinuteTotals::default, |(_, totals)| *totals)
    }

    fn availability_bucket(
        handle: &DatabaseHandle,
        provider: LlmProviderId,
        minute: i64,
    ) -> AvailabilityCounts {
        wait(handle.availability_minute(provider, "model-a".into(), minute))
            .expect("read availability minute")
            .value
            .unwrap_or_default()
    }

    fn classification(handle: &DatabaseHandle, request: RequestRef) -> AvailabilityClassification {
        wait(handle.request_sample(request))
            .expect("read the request sample")
            .value
            .expect("a sample")
            .sample
            .classification
    }

    fn token_sample(handle: &DatabaseHandle, request: RequestRef) -> TokenRequestSample {
        wait(handle.token_request(request))
            .expect("read the Token request")
            .value
            .expect("a request state")
            .sample
            .expect("an eligible sample")
    }

    #[test]
    fn the_requests_of_one_job_are_reduced_and_counted_apart() {
        let (_root, directory) = data_directory("per-request-evidence");
        let worker = DatabaseWorker::start(&directory).expect("start database");
        let handle = worker.handle();
        let (provider, llm) = llm_setup(&handle, "model-a");
        let job = llm_job(&handle, llm);
        let micros = at_minute(7);
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

        drain(&handle);
        assert_eq!(
            classification(&handle, request(job, 1)),
            AvailabilityClassification::Unconfirmed
        );
        assert_eq!(
            classification(&handle, request(job, 2)),
            AvailabilityClassification::Available
        );
        assert_eq!(token_sample(&handle, request(job, 2)).total, Some(27));
        let minute = minute_of(at(micros));
        let totals = token_bucket(&handle, provider, minute);
        assert_eq!(
            (
                totals.requests,
                totals.input.final_sum,
                totals.total.final_sum
            ),
            (2, 30, 42)
        );
        let counts = availability_bucket(&handle, provider, minute);
        assert_eq!((counts.available, counts.unconfirmed), (1, 1));
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
        let executor =
            executor(handle, "llm worker", ExecutorType::Llm, &[OPENAI]).expect("register");
        let job = llm_job(handle, llm);
        wait(handle.start_local_job(job)).expect("start the Job locally");
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
                stop_unconfirmed: None,
            }) => (*receipt, stop),
            other => panic!("expected the tool loop to stop, got {other:?}"),
        }
    }

    #[test]
    fn an_authorized_tool_runs_and_its_result_returns_for_the_final_answer() {
        let (_root, directory) = data_directory("tool-loop");
        let worker = DatabaseWorker::start(&directory).expect("start database");
        let handle = worker.handle();
        let (provider, llm) = llm_setup(&handle, "model-a");
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
        drain(&handle);
        assert_eq!(
            classification(&handle, request(run.job, 2)),
            AvailabilityClassification::Available
        );
        assert_eq!(token_sample(&handle, request(run.job, 1)).total, Some(15));
        let _ = provider;
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
        let ambiguous = r#"{"index":0,"id":"c","function":{"name":"add","arguments":"{\"a\":1,\"b\":2}"}},{"index":0,"function":{"name":"add"}}"#;
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
        let executor =
            executor(&handle, "llm worker", ExecutorType::Llm, &[OPENAI]).expect("register");
        let markers = || {
            wait(handle.operation_markers(None, 1_000))
                .expect("markers")
                .len()
        };
        let before = markers();
        let runtime = ExecutionRuntime::start(handle.clone()).expect("start runtime");
        let mut plan = ProviderExecutionPlan::environment_credentials(
            executor,
            JobInputDraft::default(),
            NewJob {
                configuration: llm_only(llm),
                ..new_job("tool plan", handle.genesis(), Vec::new())
            },
        );
        plan.tools = Some(execution(&registry, &["add"], zero_requests));
        assert!(matches!(
            wait(runtime.scheduler().schedule_provider(plan)),
            Err(ExecutionRuntimeError::InvalidToolLimits(
                ToolLimitsError::NoProviderRequest
            ))
        ));
        runtime.shutdown().expect("shutdown runtime");
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
        let executor =
            executor(&handle, "llm worker", ExecutorType::Llm, &[OPENAI]).expect("register");
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
                ..new_job("tool plan", handle.genesis(), Vec::new())
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
        runtime.shutdown().expect("shutdown runtime");
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
        wait(handle.confirm_job_stopped(job)).expect("confirm the stop");
    }

    fn retry_of(original: JobId, llm: ProfileId, acknowledge_repeated_effects: bool) -> JobRetry {
        JobRetry {
            original,
            input: RetryInput::Unchanged,
            name: Some("retry".into()),
            execution_mode: ExecutionMode::Once,
            eligible_at: None,
            scheduled_at: None,
            next_due_at: None,
            next_iteration_no: None,
            iteration_no: None,
            projects: Vec::new(),
            dependencies: Vec::new(),
            configuration: llm_only(llm),
            assigned_executor: None,
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
            assert_eq!(handle.abandoned_at_open(), [run.job]);
            let job = job_record(&handle, run.job);
            // The opening is gone, so the outcome is unknown. Whether the work
            // it admitted has stopped was never established, so the stop stays
            // unconfirmed and the Profiles it still holds are not released.
            assert!(job.unknown_at.is_some() && job.stop_confirmed_at.is_none());
            assert_eq!(blocking(&handle, llm), [run.job]);
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
                job.unknown_at.is_some() && job.stop_confirmed_at.is_none(),
                "{case}: unknown, and not confirmed stopped"
            );
            assert_eq!(
                blocking(&handle, llm),
                [run.job],
                "{case}: the Profile is still held by work of unknown stop"
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
                    Violation::RetryOfUnsettledJob(run.job),
                    "{case}: the original is unsettled, so no acknowledgement \
                     reaches the unresolved effect behind it"
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
            assert!(
                !has_outcome(&job),
                "neither Completed nor Failed was claimed"
            );
            worker.shutdown().expect("shutdown");

            let worker = DatabaseWorker::start_with_config(&directory, lenient()).expect("restart");
            let handle = worker.handle();
            let job = job_record(&handle, run.job);
            assert!(job.unknown_at.is_some() && job.stop_confirmed_at.is_none());
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
            let run = observed_run(&handle, llm, 1);
            let call = tool_call(run.job, 1, 0);
            wait(handle.record_tool_decision(admitted(call, "c", ToolEffect::ReadOnly)))
                .expect("admit");
            if let Some(outcome) = recorded.clone() {
                wait(handle.record_tool_outcome(call, outcome)).expect("record");
            }
            for finalization in [
                JobFinalization::Completed(result("done")),
                JobFinalization::Failed(result("failed")),
                JobFinalization::Cancelled(result("cancelled")),
            ] {
                assert_eq!(
                    violation(wait(handle.finalize_job(run.job, finalization))),
                    Violation::UnresolvedToolEffect(call),
                    "{recorded:?}"
                );
            }
            assert!(
                blocking(&handle, llm).contains(&run.job),
                "it still holds its Profile"
            );
            settle_unknown(&handle, run.job);
            assert!(
                !blocking(&handle, llm).contains(&run.job),
                "its confirmed stop releases it"
            );
            let records = evidence(&handle, run.job);
            assert_eq!(
                outcome_of(&records[0]),
                recorded,
                "the stop changes no outcome"
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
        let looped = run_loop(
            &handle,
            run,
            Some(execution(&registry, &["echo"], ToolLoopLimits::default())),
            vec![
                calling_in_fragments(&[(0, "a", "echo", sixty()), (1, "b", "echo", sixty())]),
                calling_in_fragments(&[
                    (0, "c", "echo", sixty()),
                    (1, "d", "echo", sixty()),
                    (2, "e", "echo", sixty()),
                ]),
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
            2,
            "no call of the response over the limit ran"
        );
        let records = evidence(&handle, run.job);
        assert_eq!(
            records
                .iter()
                .map(|record| (
                    record.call,
                    record.outcome.as_ref().map(|outcome| outcome.fact.kind())
                ))
                .collect::<Vec<_>>(),
            [
                (tool_call(run.job, 1, 0), Some(ToolOutcomeKind::Completed)),
                (tool_call(run.job, 1, 1), Some(ToolOutcomeKind::Completed)),
                (tool_call(run.job, 2, 2), None),
            ]
        );
        assert_eq!(
            records[2].decision.fact,
            ToolDecision::Refused(ToolCallRefusal::LimitExceeded)
        );
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
        wait(handle.finalize_job(retry, JobFinalization::Failed(result("no tool ran"))))
            .expect("fail the retry");
        retry
    }

    /// A retry of `original` with a fresh input, which must be refused for
    /// `expected` and leave nothing behind: no Job, no submitted input and no
    /// RETRY_OF edge.
    fn assert_retry_refused(
        handle: &DatabaseHandle,
        llm: ProfileId,
        original: JobId,
        acknowledge: bool,
        expected: Violation,
    ) {
        let jobs = wait(handle.jobs(None, 1_000)).expect("jobs").len();
        let input = wait(handle.save_job_input(None, JobInputDraft::default())).expect("input");
        let retry = JobRetry {
            input: RetryInput::Changed(input),
            ..retry_of(original, llm, acknowledge)
        };
        assert_eq!(violation(wait(handle.retry_job(retry))), expected);
        assert_eq!(wait(handle.jobs(None, 1_000)).expect("jobs").len(), jobs);
        assert!(wait(handle.retries(original)).expect("retries").is_empty());
        let input = wait(handle.job_input(input))
            .expect("read the input")
            .expect("the input");
        assert!(input.submitted_at.is_none() && input.configuration.is_none());
        assert!(wait(handle.input_jobs(input.id)).expect("jobs").is_empty());
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
                assert!(job.unknown_at.is_some() && job.stop_confirmed_at.is_none());
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
                    Violation::RetryOfUnsettledJob(run.job)
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
            assert!(job.stop_confirmed_at.is_none());
            let [record] = evidence(&handle, run.job).try_into().expect("one call");
            assert!(record.outcome.is_none() && record.is_unresolved());
            assert_eq!(
                violation(wait(handle.retry_job(retry_of(run.job, llm, true)))),
                // The original's execution is not confirmed stopped, so it is
                // not settled and cannot be retried at all. That refusal now
                // precedes the unresolved-effect one, and is the stronger of
                // the two: acknowledging the effect would not lift it.
                Violation::RetryOfUnsettledJob(run.job)
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
                    // Unsettled, because its stop was never confirmed, so the
                    // retry is refused before the unresolved effect is even
                    // considered, and repeated restarts do not change that.
                    Violation::RetryOfUnsettledJob(run.job)
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
                    crate::orchestration::workspace_index_query::TOOL_NAME,
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
                assert_eq!(admission.handler_version, "workspace_run_command-2");
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
            let executor =
                executor(handle, "llm worker", ExecutorType::Llm, &[OPENAI]).expect("register");
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
                    ..new_job(name, handle.genesis(), Vec::new())
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
            assert_eq!(record.assigned_executor, Some(target.executor));
            assert!(record.started_at.is_some() && !settled(&record));
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
            runtime.shutdown().expect("shutdown runtime");
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
            runtime.shutdown().expect("shutdown runtime");
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
            runtime.shutdown().expect("shutdown runtime");
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
            let companion_job = llm_job(&handle, target.llm);
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
            runtime.shutdown().expect("shutdown runtime");

            // Whatever the commit did, there is at most that one Job, and a
            // later start settles it if it exists; nothing created another.
            wait(handle.recover_storage()).expect("recover storage");
            let status = wait(handle.reconcile_operation(operation)).expect("reconcile");
            let jobs = jobs_named(&handle, "unknown commit");
            match status {
                crate::database::OperationStatus::Committed(_) => {
                    let [record] = jobs.try_into().expect("the one Job");
                    assert!(record.started_at.is_some() && !settled(&record));
                    worker.shutdown().expect("shutdown");
                    let worker = DatabaseWorker::start(&directory).expect("restart");
                    assert_eq!(worker.handle().abandoned_at_open(), [record.id]);
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
            runtime.shutdown().expect("shutdown runtime");
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
            runtime.shutdown().expect("shutdown runtime");
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
                    record.failed_at.is_some()
                        && record.unknown_at.is_none()
                        && record.cancelled_at.is_none(),
                    "{record:?}"
                );
            }
            assert_eq!(requests.load(Ordering::SeqCst), 1, "nothing else was sent");
            runtime.shutdown().expect("shutdown runtime");
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
            runtime.shutdown().expect("shutdown runtime");
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
            runtime.shutdown().expect("shutdown runtime");
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
            runtime.shutdown().expect("shutdown runtime");
            stopping.join().expect("joined");
            assert!(matches!(
                queued(),
                Err(AdmissionError::NotAdmitted(
                    ExecutionRuntimeError::ShuttingDown
                ))
            ));
            assert!(jobs_named(&handle, "queued").is_empty());
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
            runtime.shutdown().expect("shutdown runtime");
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
                    let provider = database
                        .create_llm_provider(NewLlmProvider {
                            name: endpoint.into(),
                            transport: OPENAI,
                            endpoint: Some(format!("https://{endpoint}.invalid/v1")),
                            credential_ref: None,
                        })
                        .await
                        .expect("create provider");
                    let llm = database
                        .create_profile(NewProfile {
                            name: "hosted".into(),
                            config: NewProfileConfig::Llm {
                                provider,
                                config: llm_config("hosted", &[ModelCapability::Chat]),
                            },
                        })
                        .await
                        .expect("create profile");
                    let executor = database
                        .create_executor(NewExecutor {
                            name: "hosted worker".into(),
                            executor_type: ExecutorType::Llm,
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
                    assert!(record.started_at.is_some() && !settled(&record));
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
            let executor =
                executor(handle, "llm worker", ExecutorType::Llm, &[OPENAI]).expect("register");
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
                self.runtime.shutdown().expect("shutdown runtime");
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
            let executor =
                executor(handle, "llm worker", ExecutorType::Llm, &[OPENAI]).expect("register");
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

            let genesis = get(
                address,
                &format!("/api/v1/jobs/{}/input", running.handle.genesis()),
            );
            assert_eq!(genesis.status, 200, "{}", genesis.body);
            assert_eq!(genesis.json()["input"]["configuration"], Value::Null);
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
            assert!(record.started_at.is_some() && !settled(&record));
            assert_eq!(record.assigned_executor, Some(target.executor));
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
            let report = run_controlled_reporting(
                directory,
                false,
                Some(config(admission_wait)),
                move |context: HostContext| {
                    let address = context.control_plane().expect("listening");
                    async move {
                        let value = std::thread::spawn(move || client(address))
                            .join()
                            .expect("the client did not panic");
                        *slot.lock().expect("lock") = Some(value);
                    }
                },
            );
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
            let (done, left) = hosted(&directory, SHORT, move |address| {
                let (done_body, running_body, queued_body) = bodies;
                let done = admitted(address, done_key, &done_body);
                until(|| state(address, done) == "completed");
                let left = admitted(address, running_key, &running_body);
                until(|| requests.load(Ordering::SeqCst) == 2);
                // The executor is busy, so this one is only queued when the
                // host stops: it is pending, never admitted.
                let queued = submit(address, queued_key, &queued_body);
                assert_eq!(queued.status, 202, "{}", queued.body);
                assert_eq!(queued.admission(), ("pending".to_owned(), None));
                assert_eq!(lookup(address, queued_key).admission().0, "pending");
                (done, left)
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
                assert_eq!(lookup(address, queued_key).admission().0, "not_found");
                let queued = submit(address, queued_key, &queued_body);
                assert_eq!(queued.status, 201, "{}", queued.body);
                let queued = queued.job();
                until(|| requests.load(Ordering::SeqCst) == 3);
                (done, queued)
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
            assert_eq!(target.requests(), 3, "nothing ran twice");
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
            until(|| lookup(address, before_key).admission().0 == "pending");
            drop(connection);
            // A retry while it is pending waits on the same admission.
            let retry = submit(address, before_key, &before_body);
            assert_eq!(
                (retry.status, retry.admission().0.as_str()),
                (202, "pending")
            );
            assert!(jobs_named(&handle, "c2 lost before").is_empty());
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
            let companion = llm_job(&handle, target.llm);
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
                    assert!(record.started_at.is_some() && !settled(&record));
                    let replay = submit(address, key, &body);
                    assert_eq!((replay.status, replay.job()), (200, record.id));
                    // Nothing executes it in this process; it is not stoppable here.
                    assert_eq!(stop(address, record.id).error(), "not_stoppable");
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
            assert_eq!(pending.admission().0, "pending");

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
            // and an unknown one is not found.
            let foreign = llm_job(&handle, target.llm);
            for job in [foreign, handle.genesis()] {
                let refused = stop(address, job);
                assert_eq!(
                    (refused.status, refused.error().as_str()),
                    (409, "not_stoppable")
                );
            }
            assert!(!settled(&job_record(&handle, foreign)));
            let missing = stop(address, JobId::new(999_999));
            assert_eq!(
                (missing.status, missing.error().as_str()),
                (404, "job_not_found")
            );
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
            runtime.shutdown().expect("shutdown runtime");
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
            let system =
                executor(&handle, "system worker", ExecutorType::System, &[]).expect("register");
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
            let input = wait(handle.save_job_input(
                None,
                JobInputDraft {
                    objective: Some("bound elsewhere".into()),
                    ..JobInputDraft::default()
                },
            ))
            .expect("save input");
            let bound = wait(handle.create_submitted_local_job(
                NewJob {
                    configuration: llm_only(target.llm),
                    ..new_job("c2f1 bound elsewhere", handle.genesis(), Vec::new())
                },
                input,
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

        #[test]
        fn the_submission_index_is_added_to_a_format_13_file_and_enforced_by_its_transaction() {
            use crate::persistence::{NewSubmission, RequestFingerprint, SubmissionKey};

            let (_root, directory) = data_directory("c2-format");
            let worker = DatabaseWorker::start(&directory).expect("start");
            let handle = worker.handle();
            let llm = llm_profile(
                &handle,
                provider(&handle, "c2-format"),
                llm_config("c2-format", &[ModelCapability::Chat]),
            );
            worker.shutdown().expect("shutdown");
            // As the previous build left it: format 13, without the index.
            let file = crate::storage::database_file(directory.database_path());
            {
                let database = redb::Database::create(&file).expect("open the file");
                let tx = database.begin_write().expect("write");
                assert!(
                    tx.delete_table(redb::TableDefinition::<&[u8], &[u8]>::new(
                        "control_submission"
                    ))
                    .expect("delete the index")
                );
                tx.commit().expect("commit");
            }

            let worker = DatabaseWorker::start(&directory).expect("a format 13 file opens");
            let handle = worker.handle();
            let key = SubmissionKey::from_digest([7; 32]);
            let fingerprint = RequestFingerprint::from_digest([1; 32]);
            assert!(matches!(wait(handle.submission(key)), Ok(None)));
            let create = |fingerprint| {
                let input = wait(handle.save_job_input(
                    None,
                    JobInputDraft {
                        objective: Some("c2 format".into()),
                        ..JobInputDraft::default()
                    },
                ))
                .expect("save input");
                wait(handle.create_submitted_local_job(
                    NewJob {
                        configuration: llm_only(llm),
                        ..new_job("c2 format", handle.genesis(), Vec::new())
                    },
                    input,
                    NewSubmission { key, fingerprint },
                ))
            };
            let job = create(fingerprint).expect("created with its key");
            let record = wait(handle.submission(key))
                .expect("read")
                .expect("recorded");
            assert_eq!((record.job, record.fingerprint), (job, fingerprint));
            assert!(job_record(&handle, job).started_at.is_some());
            // The transaction refuses the key again, whatever the request,
            // and names the Job it created; nothing more is committed.
            for (other, same_request) in [
                (fingerprint, true),
                (RequestFingerprint::from_digest([2; 32]), false),
            ] {
                assert_eq!(
                    violation(create(other)),
                    Violation::SubmissionKeyUsed { job, same_request }
                );
            }
            assert_eq!(jobs_named(&handle, "c2 format").len(), 1);
            worker.shutdown().expect("shutdown");

            let worker = DatabaseWorker::start(&directory).expect("reopen");
            assert_eq!(
                wait(worker.handle().submission(key)).expect("read"),
                Some(record),
                "the record is durable and unchanged"
            );
            worker.shutdown().expect("shutdown");
            assert_relationships_consistent(&directory);
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
            handle: DatabaseHandle,
            runtime: ExecutionRuntime,
            control: ControlPlane,
        }

        impl Running {
            fn start(directory: &DataDirectory, config: ControlPlaneConfig) -> Self {
                let worker =
                    DatabaseWorker::start_with_config(directory, lenient()).expect("start");
                let handle = worker.handle();
                let runtime = ExecutionRuntime::start(handle.clone()).expect("start runtime");
                let control = ControlPlane::start(config, handle.clone(), runtime.scheduler())
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

            fn open_streams(&self) -> usize {
                self.control.streams().0
            }

            fn stop(self) {
                wait(self.control.stop()).expect("stop the listener");
                self.runtime.shutdown().expect("shutdown runtime");
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
            let job = llm_job(handle, llm);
            wait(handle.start_local_job(job)).expect("start");
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
            let executor =
                executor(handle, "llm worker", ExecutorType::Llm, &[OPENAI]).expect("register");
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

            // Unknown, its local execution confirmed stopped: concluded,
            // with no result it never had.
            let confirmed = running_job(&handle);
            progress(&handle, confirmed, "partial");
            wait(handle.finalize_job(confirmed, JobFinalization::Unknown)).expect("unknown");
            wait(handle.confirm_job_stopped(confirmed)).expect("confirm");
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
            let unconfirmed = running_job(&handle);
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
            let unknown = running_job(&handle);
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
                handle,
                runtime,
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
            runtime.shutdown().expect("shutdown runtime");
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

        fn cleanup_markers(handle: &DatabaseHandle) -> Vec<crate::persistence::OperationMarker> {
            wait(handle.operation_markers(None, 1_000))
                .expect("markers")
                .into_iter()
                .filter(|marker| {
                    marker.kind == crate::persistence::OperationKind::CleanupAttachment
                })
                .collect()
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
            let executor =
                executor(handle, "llm worker", ExecutorType::Llm, &[OPENAI]).expect("register");
            let input = wait(handle.save_job_input(
                None,
                JobInputDraft {
                    objective: Some(OBJECTIVE.to_owned()),
                    attachments,
                    ..JobInputDraft::default()
                },
            ))
            .expect("save the input");
            let job = wait(handle.create_local_job_with_input(
                NewJob {
                    configuration: llm_only(llm),
                    ..new_job("vision", handle.genesis(), Vec::new())
                },
                input,
            ))
            .expect("submit the Job");
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
            assert_eq!(crate::storage::FORMAT_VERSION, 15);
            let worker = DatabaseWorker::start(&directory).expect("a new format 15 database");
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
            let marker = wait(handle.operation_markers(None, 1_000))
                .expect("markers")
                .into_iter()
                .find(|marker| marker.kind == crate::persistence::OperationKind::CreateAttachment)
                .expect("a marker");
            assert_eq!(marker.created, Some(RecordRef::Attachment(first)));
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
        fn an_input_names_its_attachments_in_order_until_it_is_submitted() {
            let (_root, directory) = data_directory("attachment-input");
            let worker = DatabaseWorker::start(&directory).expect("start");
            let handle = worker.handle();
            let (_provider, llm) = llm_setup(&handle, "model-a");
            let a = attach(&handle, png(), "image/png", "a.png");
            let b = attach(&handle, jpeg(), "image/jpeg", "b.jpg");
            let c = attach(&handle, other_png(), "image/png", "c.png");
            let draft = |attachments: Vec<AttachmentId>| JobInputDraft {
                objective: Some(OBJECTIVE.to_owned()),
                attachments,
                ..JobInputDraft::default()
            };
            let input = wait(handle.save_job_input(None, draft(vec![c, a, b]))).expect("save");
            assert_eq!(
                input_attachments(&handle, input),
                [c, a, b],
                "in the given order"
            );
            wait(handle.save_job_input(Some(input), draft(vec![b, c]))).expect("edit");
            assert_eq!(input_attachments(&handle, input), [b, c]);
            assert!(wait(handle.attachment_inputs(a)).expect("read").is_empty());
            assert_eq!(wait(handle.attachment_inputs(b)).expect("read"), [input]);

            let missing = AttachmentId::new(42);
            assert_eq!(
                violation(wait(
                    handle.save_job_input(Some(input), draft(vec![a, missing]))
                )),
                Violation::NotFound(RecordRef::Attachment(missing))
            );
            assert_eq!(
                violation(wait(handle.save_job_input(None, draft(vec![a, b, a])))),
                Violation::DuplicateReference(RecordRef::Attachment(a))
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
                violation(wait(handle.save_job_input(Some(input), draft(vec![a])))),
                Violation::AttachmentCleanedUp(a)
            );
            assert_eq!(
                input_attachments(&handle, input),
                [b, c],
                "refusals changed nothing"
            );

            let job = wait(handle.create_job_with_input(
                NewJob {
                    configuration: llm_only(llm),
                    ..new_job("vision", handle.genesis(), Vec::new())
                },
                input,
            ))
            .expect("submit");
            assert_eq!(
                violation(wait(handle.save_job_input(Some(input), draft(vec![c])))),
                Violation::InputAlreadySubmitted(input)
            );
            let submitted = wait(handle.job_input_for_job(job))
                .expect("read")
                .expect("input");
            assert_eq!((submitted.id, submitted.attachments), (input, vec![b, c]));
            assert!(submitted.submitted_at.is_some());
            worker.shutdown().expect("shutdown");

            let worker = DatabaseWorker::start(&directory).expect("restart");
            assert_eq!(
                input_attachments(&worker.handle(), input),
                [b, c],
                "frozen with the input"
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
            let [marker] = cleanup_markers(&handle)
                .try_into()
                .expect("one cleanup marker");
            assert_eq!(
                (marker.subject, marker.created),
                (RecordRef::Attachment(cleaned), None)
            );
            let cleanup = AttachmentCleanup {
                kind: CleanupKind::Manual,
                cleaned_at: marker.committed_at,
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
            assert_eq!(cleanup_markers(&handle), std::slice::from_ref(&marker));

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
            assert_eq!(cleanup_markers(&handle), [marker]);
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
            assert!(cleanup_markers(&handle).is_empty());
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
            assert_eq!(cleanup_markers(&handle).len(), 2);

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
            assert!(cleanup_markers(&handle).is_empty());
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
        fn a_format_12_database_is_refused_without_changing_it_or_its_attachments() {
            let (_root, directory) = data_directory("attachment-format-12");
            let worker = DatabaseWorker::start(&directory).expect("start");
            worker.shutdown().expect("shutdown");
            let file = crate::storage::database_file(directory.database_path());
            {
                let database = redb::Database::create(&file).expect("open the file");
                let tx = database.begin_write().expect("write");
                tx.open_table(redb::TableDefinition::<&str, &[u8]>::new("meta"))
                    .expect("meta")
                    .insert("format_version", 12u32.to_le_bytes().as_slice())
                    .expect("set format 12");
                tx.commit().expect("commit");
            }
            let staging = directory.attachments_path().join(".ocg-staging-1-2-3");
            std::fs::write(&staging, b"x").expect("write");
            let bytes = std::fs::read(&file).expect("read");
            match DatabaseWorker::start(&directory) {
                Err(DatabaseError::UnsupportedFormat { found, supported }) => {
                    assert_eq!((found, supported), (12, 15));
                }
                Err(other) => panic!("expected a format refusal, got {other}"),
                Ok(_) => panic!("a format 12 database must be refused"),
            }
            assert_eq!(std::fs::read(&file).expect("reread"), bytes, "unchanged");
            assert!(
                staging.exists(),
                "nothing is cleaned for a refused database"
            );
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
                original: run.job,
                input: RetryInput::Unchanged,
                name: Some("retry".into()),
                execution_mode: ExecutionMode::Once,
                eligible_at: None,
                scheduled_at: None,
                next_due_at: None,
                next_iteration_no: None,
                iteration_no: None,
                projects: Vec::new(),
                dependencies: Vec::new(),
                configuration: llm_only(llm),
                assigned_executor: None,
                acknowledge_repeated_effects: false,
            }))
            .expect("retry");
            let retried = wait(handle.job_input_for_job(retry))
                .expect("read")
                .expect("input");
            assert_eq!(retried, input, "the same input and Attachments");
            assert_eq!(persisted_body(&handle, retry).expect("a body"), expected);
            assert_eq!(
                wait(handle.attachments(None, 10)).expect("list").len(),
                1,
                "no new Attachment"
            );
            assert_eq!(
                wait(handle.attachment_inputs(id)).expect("read"),
                [run.input]
            );
            assert_eq!(
                wait(handle.input_jobs(run.input)).expect("read"),
                [run.job, retry]
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
            let provider = wait(handle.create_llm_provider(NewLlmProvider {
                name: "silent".into(),
                transport: OPENAI,
                endpoint: Some(endpoint),
                credential_ref: None,
            }))
            .expect("create provider");
            let llm = llm_profile(
                &handle,
                provider,
                llm_config("model-v", &[ModelCapability::Chat]),
            );
            let executor =
                executor(&handle, "llm worker", ExecutorType::Llm, &[OPENAI]).expect("register");
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
                                ..new_job("vision", handle.genesis(), Vec::new())
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
            let earlier = wait(handle.save_job_input(
                None,
                JobInputDraft {
                    attachments: vec![cleaned],
                    ..JobInputDraft::default()
                },
            ))
            .expect("save");
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
            runtime.shutdown().expect("shutdown runtime");
            worker.shutdown().expect("shutdown");
            assert_relationships_consistent(&directory);
        }
    }
}
